import json
import os
import re
import traceback
from abc import ABC
from datetime import datetime

from model_library.base import *

from .logger import get_logger
from .tool import Tool
from .utils import INSTRUCTIONS_PROMPT, _merge_statistics

agent_logger = get_logger(__name__)


class ModelException(Exception):
    """
    Raised on model errors
    not retried by default
    """

    pass


class ToolCallException(Exception):
    """
    raised when tool call str doesn't parse to json
    prev message is deleted and retried
    """

    pass


class Agent(ABC):
    def __init__(
        self,
        tools: dict[str, Tool],
        llm: LLM,
        max_turns: int = 20,
        instructions_prompt: str = INSTRUCTIONS_PROMPT,
    ):
        self.tools = tools
        self.llm = llm
        self.max_turns = max_turns
        self.instructions_prompt = instructions_prompt

    async def _process_turn(self, turn_count, data_storage, metadata):
        """
        Process a single turn in the agent's conversation.

        Args:
            turn_count (int): The current turn number
            data_storage (dict): Storage for conversation data
            metadata (dict): Session metadata

        Returns:
            tuple: (final_answer, turn_metadata, should_continue)
        """
        agent_logger.info(f"\033[1;34m[TURN {turn_count}]\033[0m")

        # Get response from LLM
        tool_definitions = [tool.get_tool_repr() for tool in self.tools.values()]
        agent_logger.info(
            f"\033[1;35m[TOOLS AVAILABLE]\033[0m {[tool.name for tool in tool_definitions]}"
        )
        try:
            response: QueryResult = await self.llm.query(
                input=self.messages, tools=tool_definitions
            )
        except Exception as e:
            agent_logger.critical(f"Error: {e}")
            agent_logger.critical(f"Traceback: {traceback.format_exc()}")
            raise ModelException(e)

        # record response
        # TODO: make this less hacky
        self.messages = response.history

        # parse QueryResult
        response_text = response.output_text
        reasoning_text = response.reasoning
        tool_calls: list[ToolCall] = response.tool_calls

        agent_logger.info(
            f"\033[1;36m[TOOL CALLS RECEIVED]\033[0m {len(tool_calls)} tool calls: {[tc.name for tc in tool_calls]}"
        )

        # record turn metadata
        turn_metadata = response.metadata.model_dump()
        turn_metadata["tool_calls"] = []
        turn_metadata["errors"] = []
        turn_metadata["tokens_retrieval"] = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }

        # Log the thinking content if available
        if reasoning_text:
            agent_logger.info(f"\033[1;33m[LLM REASONING]\033[0m {reasoning_text}")

        if response_text:
            agent_logger.info(f"\033[1;33m[LLM THINKING]\033[0m {response_text}")

        if tool_calls:
            for tool_call in tool_calls:
                tool_name = tool_call.name

                # unpacks tool call arguments
                arguments = tool_call.args
                if isinstance(arguments, str):
                    try:
                        arguments = json.loads(arguments)
                    except json.JSONDecodeError:
                        agent_logger.warning(
                            f"Could not parse tool call arguments: {arguments}"
                        )
                        raise ToolCallException(
                            f"Could not parse tool call arguments: {arguments}"
                        )

                # Track tool call in turn metadata
                tool_call_metadata = {
                    "tool_name": tool_name,
                    "arguments": arguments,
                    "success": False,
                    "error": None,
                }
                if tool_name not in self.tools:
                    error_msg = f"Tool '{tool_name}' not found. Available tools: {list(self.tools.keys())}"

                    # Update error tracking
                    tool_call_metadata["error"] = error_msg
                    turn_metadata["errors"].append(error_msg)

                    # Add error to messages
                    tool_result = ToolResult(tool_call=tool_call, result=error_msg)
                    self.messages.append(tool_result)
                    continue

                # Call tools with appropriate arguments
                if tool_name == "retrieve_information":
                    tool_result = await self.tools[tool_name](
                        arguments, data_storage, self.llm
                    )
                    if "usage" in tool_result:
                        tool_token_usage = tool_result["usage"]
                        turn_metadata["in_tokens"] += tool_token_usage["prompt_tokens"]
                        turn_metadata["out_tokens"] += tool_token_usage[
                            "completion_tokens"
                        ]
                        tr = turn_metadata.get("tokens_retrieval")
                        if tr is None:
                            tr = {
                                "prompt_tokens": 0,
                                "completion_tokens": 0,
                                "total_tokens": 0,
                            }
                            turn_metadata["tokens_retrieval"] = tr
                        tr["prompt_tokens"] += tool_token_usage["prompt_tokens"]
                        tr["completion_tokens"] += tool_token_usage["completion_tokens"]
                        tr["total_tokens"] += tool_token_usage["total_tokens"]
                elif tool_name == "parse_html_page":
                    tool_result = await self.tools[tool_name](arguments, data_storage)
                else:
                    tool_result = await self.tools[tool_name](arguments)

                if tool_result["success"]:
                    # Add tool result to messages
                    tool_call_metadata["success"] = True
                else:
                    tool_call_metadata["error"] = tool_result["result"]
                    turn_metadata["errors"].append(tool_result["result"])

                tool_result_obj = ToolResult(
                    tool_call=tool_call, result=tool_result["result"]
                )
                self.messages.append(tool_result_obj)

                # Add tool call metadata to turn
                turn_metadata["tool_calls"].append(tool_call_metadata)

        else:
            # Detect "FINAL ANSWER:" in pure text
            final_answer_pattern = re.compile(r"FINAL ANSWER:", re.IGNORECASE)

            if isinstance(response_text, str) and final_answer_pattern.search(
                response_text
            ):
                final_answer_match = re.search(
                    r"FINAL ANSWER:(.*?)(?:\{\"sources\"|\Z)",
                    response_text,
                    re.DOTALL,
                )
                sources_match = re.search(
                    r"(\{\"sources\".*\})", response_text, re.DOTALL
                )

                answer_text = (
                    final_answer_match.group(1).strip() if final_answer_match else ""
                )

                sources_text = sources_match.group(1) if sources_match else ""

                final_answer = answer_text
                if sources_text:
                    final_answer = f"{answer_text}\n\n{sources_text}"

                agent_logger.info(f"\033[1;32m[FINAL ANSWER]\033[0m {final_answer}")

                return final_answer, turn_metadata, False
            else:
                agent_logger.info(f"\033[1;33m[LLM THINKING]\033[0m {response_text}")

        return None, turn_metadata, True

    async def run(self, question: str) -> tuple[str, dict[str, Any]]:
        """
        Run the agent on a question from the user.

        Args:
            question (str): The user's question
            session_id (str, optional): A unique identifier for this session

        Returns:
            tuple[str, dict]: The final answer and metadata about the run
        """
        # Initialize metadata
        metadata = {
            "user_input": question,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "total_duration_seconds": 0,
            "total_tokens": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
            "total_tokens_retrieval": {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
            "turns": [],
            "tool_usage": {},
            "tool_calls_count": 0,
            "api_calls_count": 0,
            "error_count": 0,
        }

        # Initialize data storage for this conversation
        data_storage = {}

        # Prepare initial message with instructions
        initial_prompt = self.instructions_prompt.format(question=question)

        initial_message = TextInput(text=initial_prompt)
        self.messages: list[InputItem] = [initial_message]

        agent_logger.info(f"\033[1;34m[USER INSTRUCTIONS]\033[0m {initial_prompt}")

        turn_count = 0
        final_answer = None

        while turn_count < self.max_turns:
            turn_count += 1
            # Process the current turn
            try:
                result, turn_metadata, should_continue = await self._process_turn(
                    turn_count, data_storage, metadata
                )

                # Add turn metadata to session metadata
                metadata["turns"].append(turn_metadata)

            # Handle ModelException
            except ModelException as e:
                agent_logger.error(f"\033[1;31m[DO NOT RETRY]\033[0m {e}")
                should_continue = False

            # kimi messes up tool calls
            except ToolCallException:
                last_message = self.messages.pop(-1)
                agent_logger.warning(
                    f"\033[1;37m[RETRYING TOOL CALL]\033[0m Removed last message: {last_message}"
                )

            except Exception as e:
                # Log the error
                agent_logger.error(f"\033[1;31m[ERROR]\033[0m {e}")
                agent_logger.error(
                    f"\033[1;31m[traceback]\033[0m {traceback.format_exc()}"
                )

                # Explain the error to the agent and give them a chance to recover
                error_message = TextInput(
                    text=f"An error occurred: {e}. Please review what happened and try a different approach."
                )
                self.messages.append(error_message)

                # continue in spite of the error
                should_continue = True

            # Check if we should continue
            if not should_continue:
                final_answer = result
                break

        # Finalize session metadata
        metadata["end_time"] = datetime.now().isoformat()

        if final_answer:
            metadata["final_answer"] = final_answer

        # Merge turn-level statistics into session-level statistics
        metadata = _merge_statistics(metadata)

        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)

        # Save metadata to logs/{session_id}.json
        log_path = os.path.join("logs", f"{session_id}.json")
        with open(log_path, "w") as f:
            json.dump(metadata, f, indent=2)

        if final_answer:
            return final_answer, metadata
        else:
            return "Max turns reached without final answer.", metadata
