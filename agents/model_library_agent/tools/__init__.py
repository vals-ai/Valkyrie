from .recalculate_excel import RecalculateExcelTool

TOOL_REGISTRY = {
    "recalculate_excel": lambda: RecalculateExcelTool(file_path="/workspace/template.xlsx"),
}
