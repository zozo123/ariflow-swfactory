//! Every colour and every glyph the interface uses, in one place, with a monochrome fallback.
//!
//! The boundary this module defends is the one non-negotiable that a rendering module can break on
//! its own: *status must be legible without colour*. An operator on a monochrome terminal, in a
//! screenshot pasted into a ticket, or with a red/green deficiency has to be able to tell a failed
//! job from a finished one. So no widget anywhere chooses a colour by itself — it asks here, and
//! here every state is answered with a distinct glyph as well as a style. Turn the colour off and
//! the screen still says everything it said before.

use ratatui::style::{Color, Modifier, Style};

/// The cyanotype drafting sheet `herd` used (`02-herd-tui.md` §9.2). An operator moving between
/// the two tools should recognise the paper.
pub const PAPER: Color = Color::Rgb(0x0d, 0x27, 0x40);
/// Panel backgrounds: header, tables, the log pane.
pub const GRAPHITE: Color = Color::Rgb(0x08, 0x19, 0x2b);
/// Foreground for anything that must be read first.
pub const LINE: Color = Color::Rgb(0xee, 0xf4, 0xf8);
/// Foreground for labels and chrome that must be read second.
pub const LINE_MUTE: Color = Color::Rgb(0x8a, 0xa5, 0xbb);
/// Ordinary body text: table cells, detail values.
pub const BODY: Color = Color::Rgb(0xa9, 0xc1, 0xd4);
/// Hairline rules. The layout's rhythm comes from these, not from zebra stripes.
pub const RULE: Color = Color::Rgb(0x22, 0x40, 0x5c);
/// The selected row.
pub const CURSOR: Color = Color::Rgb(0x16, 0x34, 0x4f);
/// Something is wrong and a human is the fix.
pub const STAMP: Color = Color::Rgb(0xeb, 0x6a, 0x52);
/// Something passed.
pub const OK: Color = Color::Rgb(0x74, 0xc9, 0xa1);
/// Something is waiting, degraded or merely old.
pub const WARN: Color = Color::Rgb(0xe3, 0xb3, 0x41);

/// The glyph a state that no `TASK_ORDER` word covers gets, so an unknown state is still a shape
/// and not a blank.
pub const UNKNOWN_GLYPH: &str = "?";

/// One row of the state vocabulary: the Airflow word, the glyph that stands for it, and how it
/// should feel.
///
/// The glyphs are all single-width BMP symbols on purpose. An emoji is two cells wide in some
/// terminals and one in others, which would move every column to its right depending on who is
/// looking — a table that only lines up on the author's laptop is worse than no glyph at all.
const STATES: &[(&str, &str, Tone)] = &[
    ("success", "✓", Tone::Good),
    ("failed", "✗", Tone::Bad),
    ("upstream_failed", "↯", Tone::Bad),
    ("skipped", "⊘", Tone::Muted),
    ("removed", "⊗", Tone::Muted),
    ("running", "▸", Tone::Active),
    ("queued", "·", Tone::Muted),
    ("scheduled", "∘", Tone::Muted),
    ("deferred", "◔", Tone::Waiting),
    ("awaiting_input", "◆", Tone::Waiting),
    ("restarting", "↻", Tone::Active),
    ("up_for_retry", "↺", Tone::Warn),
    ("up_for_reschedule", "⇄", Tone::Warn),
    ("none", "·", Tone::Muted),
    ("pending", "⋯", Tone::Muted),
    ("-", "–", Tone::Muted),
];

/// How a piece of information should feel, before anyone knows whether colour is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    /// It worked.
    Good,
    /// It failed, or it is blocking someone.
    Bad,
    /// It is degraded, retrying or old.
    Warn,
    /// A human is the missing dependency.
    Waiting,
    /// It is moving.
    Active,
    /// Chrome, labels, things read second.
    Muted,
    /// Ordinary text.
    Plain,
}

/// The glyph that stands for an Airflow state, distinct per state.
pub fn state_glyph(state: &str) -> &'static str {
    STATES
        .iter()
        .find(|(word, _, _)| *word == state)
        .map(|(_, glyph, _)| *glyph)
        .unwrap_or(UNKNOWN_GLYPH)
}

/// How a state should feel.
pub fn state_tone(state: &str) -> Tone {
    STATES
        .iter()
        .find(|(word, _, _)| *word == state)
        .map(|(_, _, tone)| *tone)
        .unwrap_or(Tone::Plain)
}

/// The glyph and the word together, which is what a cell renders.
///
/// Both, always. The glyph is what the eye finds while scanning a column of forty rows; the word
/// is what makes the glyph mean something to somebody who has never seen this screen before.
pub fn state_label(state: &str) -> String {
    format!("{} {state}", state_glyph(state))
}

/// The palette in force, and whether it is allowed to use colour at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Theme {
    /// False under `--no-color`, `NO_COLOR`, or a terminal that is not one.
    pub color: bool,
}

impl Default for Theme {
    fn default() -> Self {
        Self::new(true)
    }
}

impl Theme {
    /// The cyanotype sheet, or its monochrome shadow.
    pub fn new(color: bool) -> Self {
        Self { color }
    }

    /// No colour at all: every distinction below falls back to weight and reverse video.
    pub fn mono() -> Self {
        Self::new(false)
    }

    /// True when `$NO_COLOR` is set to anything, which is the convention's whole rule.
    pub fn env_allows_color() -> bool {
        std::env::var_os("NO_COLOR").is_none()
    }

    fn paint(self, fg: Color, style: Style) -> Style {
        if self.color {
            style.fg(fg)
        } else {
            style
        }
    }

    /// The screen's own background and default text.
    pub fn base(self) -> Style {
        let style = Style::default();
        if self.color {
            style.bg(PAPER).fg(BODY)
        } else {
            style
        }
    }

    /// A panel: header, table, log pane.
    pub fn panel(self) -> Style {
        let style = Style::default();
        if self.color {
            style.bg(GRAPHITE).fg(BODY)
        } else {
            style
        }
    }

    /// A field name, a column heading's neighbour, anything read second.
    pub fn label(self) -> Style {
        self.paint(LINE_MUTE, Style::default().add_modifier(Modifier::BOLD))
    }

    /// Chrome and secondary text.
    pub fn muted(self) -> Style {
        self.paint(LINE_MUTE, Style::default().add_modifier(Modifier::DIM))
    }

    /// A heading that must be read first.
    pub fn title(self) -> Style {
        self.paint(LINE, Style::default().add_modifier(Modifier::BOLD))
    }

    /// A table's column headings.
    pub fn header(self) -> Style {
        self.paint(LINE_MUTE, Style::default().add_modifier(Modifier::BOLD))
    }

    /// The hairline between panes.
    pub fn border(self) -> Style {
        self.paint(RULE, Style::default())
    }

    /// The selected row. Reverse video without colour, because a background alone disappears.
    pub fn selected(self) -> Style {
        if self.color {
            Style::default()
                .bg(CURSOR)
                .fg(LINE)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().add_modifier(Modifier::REVERSED)
        }
    }

    /// An error badge in the header: reverse video, so it survives a monochrome terminal.
    pub fn badge(self) -> Style {
        let style = Style::default().add_modifier(Modifier::REVERSED | Modifier::BOLD);
        if self.color {
            style.fg(STAMP)
        } else {
            style
        }
    }

    /// The style for a feeling.
    pub fn tone(self, tone: Tone) -> Style {
        match tone {
            Tone::Good => self.paint(OK, Style::default()),
            Tone::Bad => self.paint(STAMP, Style::default().add_modifier(Modifier::BOLD)),
            Tone::Warn => self.paint(WARN, Style::default()),
            Tone::Waiting => self.paint(WARN, Style::default().add_modifier(Modifier::BOLD)),
            Tone::Active => self.paint(LINE, Style::default()),
            Tone::Muted => self.muted(),
            Tone::Plain => self.paint(BODY, Style::default()),
        }
    }

    /// The style an Airflow state should render in.
    pub fn state(self, state: &str) -> Style {
        self.tone(state_tone(state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use unicode_width::UnicodeWidthStr;

    #[test]
    fn every_state_has_its_own_glyph() {
        let mut seen: HashSet<&str> = HashSet::new();
        for (word, glyph, _) in STATES {
            // `none`, `queued` and `pending` deliberately share the quiet dot family, so the
            // uniqueness that matters is between states an operator must tell apart.
            if ["none", "pending", "-"].contains(word) {
                continue;
            }
            assert!(seen.insert(glyph), "{word} reuses the glyph {glyph}");
        }
    }

    #[test]
    fn no_glyph_is_wider_than_one_cell() {
        for (word, glyph, _) in STATES {
            assert_eq!(
                UnicodeWidthStr::width(*glyph),
                1,
                "{word}'s glyph would shift every column to its right"
            );
        }
        assert_eq!(UnicodeWidthStr::width(UNKNOWN_GLYPH), 1);
    }

    #[test]
    fn a_state_always_renders_its_word_as_well_as_its_glyph() {
        assert_eq!(state_label("failed"), "✗ failed");
        assert_eq!(state_label("awaiting_input"), "◆ awaiting_input");
        // An Airflow release that invents a state still says the word.
        assert_eq!(state_label("hibernating"), "? hibernating");
    }

    #[test]
    fn every_state_distinction_survives_the_colour_being_off() {
        let mono = Theme::mono();
        assert!(mono.tone(Tone::Good).fg.is_none());
        assert!(mono.tone(Tone::Bad).fg.is_none());
        // Selection must not rely on a background that a monochrome terminal will not paint.
        assert!(mono.selected().add_modifier.contains(Modifier::REVERSED));
        assert!(mono.badge().add_modifier.contains(Modifier::REVERSED));
    }

    #[test]
    fn colour_is_used_when_it_is_allowed() {
        let lit = Theme::new(true);
        assert_eq!(lit.tone(Tone::Good).fg, Some(OK));
        assert_eq!(lit.tone(Tone::Bad).fg, Some(STAMP));
        assert_eq!(lit.base().bg, Some(PAPER));
    }
}
