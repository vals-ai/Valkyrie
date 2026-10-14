"""Defines the colors for `valk ui`, built around Vals's logo green (#284D32) and the docs site's palette.

Each palette becomes a Textual theme for what Textual draws (borders, cursors, scrollbars). Status glyphs, bars
and log highlighting are Rich text the UI renders itself, so they read `palette()`, which follows the app's theme,
and switching themes re-renders them. Text colors keep at least 4.5:1 contrast with the background, and the focused
border 3:1.
"""

from dataclasses import dataclass

from textual.theme import Theme

BRAND = "#284D32"


@dataclass(frozen=True)
class Palette:
    name: str
    dark: bool
    background: str
    surface: str
    panel: str
    text: str
    muted: str
    faint: str
    accent: str
    border: str
    border_focus: str
    cursor: str
    cursor_blurred: str
    hover: str
    track: str
    finished: str
    running: str
    warning: str
    error: str
    stopped: str

    def theme(self) -> Theme:
        return Theme(
            name=self.name,
            primary=self.accent,
            secondary=self.border_focus,
            accent=self.accent,
            foreground=self.text,
            background=self.background,
            surface=self.surface,
            panel=self.panel,
            success=self.finished,
            warning=self.warning,
            error=self.error,
            dark=self.dark,
            variables={
                "border": self.border_focus,
                "border-blurred": self.border,
                "text-muted": self.muted,
                "foreground-muted": self.muted,
                "block-cursor-background": self.cursor,
                "block-cursor-foreground": self.text,
                "block-cursor-text-style": "none",
                "block-cursor-blurred-background": self.cursor_blurred,
                "block-cursor-blurred-foreground": self.text,
                "block-cursor-blurred-text-style": "none",
                "block-hover-background": self.hover,
                "scrollbar": self.border,
                "scrollbar-hover": self.faint,
                "scrollbar-active": self.accent,
                "scrollbar-background": self.background,
                "scrollbar-background-hover": self.background,
                "scrollbar-background-active": self.background,
                "scrollbar-corner-color": self.background,
                "input-selection-background": f"{self.accent} 30%",
                # Textual's default is a transparent foreground, which the log can't blend, so selected text vanished.
                "screen-selection-foreground": self.text,
            },
        )


DARK = Palette(
    name="valkyrie",
    dark=True,
    background="#111412",
    surface="#161A17",
    panel="#1D231F",
    text="#E6E3DF",
    muted="#9AA7A0",
    faint="#858079",
    accent="#94C7A2",
    border="#2B322D",
    border_focus="#3F7650",
    cursor="#1C2B21",
    cursor_blurred="#171D19",
    hover="#151A16",
    track="#2B322D",
    finished="#6DB283",
    running="#8EA9D6",
    warning="#E0AE5A",
    error="#E08B87",
    stopped="#8F9692",
)

LIGHT = Palette(
    name="valkyrie-light",
    dark=False,
    background="#F3F2F2",
    surface="#EAE8E6",
    panel="#E2E0DD",
    text="#262624",
    muted="#555B57",
    faint="#6E6A66",
    accent=BRAND,
    border="#CFCBC7",
    border_focus="#386B46",
    cursor="#DCE8DF",
    cursor_blurred="#E6EBE7",
    hover="#ECEEEC",
    track="#D6D2CE",
    finished="#2E7346",
    running="#33599B",
    warning="#8A5A00",
    error="#A63A35",
    stopped="#646A66",
)

PALETTES = {palette.name: palette for palette in (DARK, LIGHT)}
_active = DARK


def palette() -> Palette:
    """Return the palette for the app's current theme."""
    return _active


def use(name: str) -> None:
    global _active
    _active = PALETTES[name]
