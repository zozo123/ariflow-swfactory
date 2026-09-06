//! The gate every byte of untrusted text passes through before a terminal sees it.
//!
//! Log lines, gate bodies, PR titles and issue text are written by agents, by CI and by strangers
//! on the internet. A terminal treats `ESC [ 2 J` as "clear the screen" and `ESC ] 0 ; … BEL` as
//! "retitle my window" no matter who wrote it, so rendering such a string is a privilege
//! escalation from *whoever produced a log line* to *whoever is watching the factory*. That is
//! non-negotiable rule 7 of the architecture, and this module is the only place it is enforced.
//!
//! The policy: drop C0 controls and DEL, keep `\t`, fold `\r\n` into `\n`, swallow whole escape
//! sequences (CSI, OSC and the other string-terminated families, in both their 7-bit `ESC x` and
//! 8-bit C1 forms) and bound the result. Stripping the sequence rather than escaping it is
//! deliberate: an OSC 8 hyperlink then leaves its visible label behind, which is what the operator
//! wanted to read anyway.
//!
//! One allocation, sized to the input. Nothing here can be made to grow output faster than it
//! consumes input — a hostile line cannot be a memory amplifier.

/// How much of a single untrusted line is worth keeping. Longer than any real log line, short
/// enough that a one-gigabyte "line" cannot be pasted into a render loop.
pub const MAX_LINE_CHARS: usize = 4096;

/// How much of an untrusted block (a gate body, an issue description) is worth keeping.
pub const MAX_BLOCK_CHARS: usize = 64 * 1024;

/// What replaces the tail when a bound is hit. Visible, one character, never mistaken for content.
pub const TRUNCATION_MARK: char = '\u{2026}';

/// Make one untrusted line safe to print. Newlines are removed: a "line" that smuggles in a `\n`
/// is trying to forge a second line of output.
pub fn sanitize_line(input: &str) -> String {
    sanitize_line_bounded(input, MAX_LINE_CHARS)
}

/// [`sanitize_line`] with an explicit bound, for a caller that knows its column budget.
pub fn sanitize_line_bounded(input: &str, max_chars: usize) -> String {
    scrub(input, false, max_chars)
}

/// Make an untrusted multi-line block safe to print, keeping its line structure.
pub fn sanitize_block(input: &str) -> String {
    sanitize_block_bounded(input, MAX_BLOCK_CHARS)
}

/// [`sanitize_block`] with an explicit bound.
pub fn sanitize_block_bounded(input: &str, max_chars: usize) -> String {
    scrub(input, true, max_chars)
}

/// The single implementation. `keep_newlines` is the only difference between a line and a block,
/// so there is only one place a control character can slip through.
fn scrub(input: &str, keep_newlines: bool, max_chars: usize) -> String {
    let mut out = String::with_capacity(input.len());
    let mut kept = 0usize;
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        let emit = match c {
            // 7-bit escape: ESC introduces a whole family of sequences.
            '\u{1b}' => {
                skip_escape(&mut chars);
                continue;
            }
            // 8-bit C1 introducers. A valid `&str` can carry these as ordinary chars.
            '\u{9b}' => {
                skip_csi_body(&mut chars);
                continue;
            }
            '\u{90}' | '\u{98}' | '\u{9d}' | '\u{9e}' | '\u{9f}' => {
                skip_string_body(&mut chars);
                continue;
            }
            // CRLF folds to LF; a lone CR is a cursor trick that rewrites what is already on
            // screen, so it goes away entirely.
            '\r' => continue,
            '\n' if keep_newlines => '\n',
            '\n' => continue,
            '\t' => '\t',
            '\u{7f}' => continue,
            c if (c as u32) < 0x20 => continue,
            c if ('\u{80}'..='\u{9f}').contains(&c) => continue,
            c => c,
        };

        if kept == max_chars {
            out.push(TRUNCATION_MARK);
            return out;
        }
        out.push(emit);
        kept += 1;
    }
    out
}

/// Consume the body of a 7-bit escape sequence, ESC already taken.
fn skip_escape(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    let Some(&next) = chars.peek() else {
        // A bare ESC at the end of the input. Dropping it alone is the safe reading.
        return;
    };
    match next {
        '[' => {
            chars.next();
            skip_csi_body(chars);
        }
        // OSC, DCS, SOS, PM, APC — all run until a string terminator.
        ']' | 'P' | 'X' | '^' | '_' => {
            chars.next();
            skip_string_body(chars);
        }
        // Charset designators and friends: intermediates then one final byte.
        c if ('\u{20}'..='\u{2f}').contains(&c) => {
            while let Some(&c) = chars.peek() {
                if ('\u{20}'..='\u{2f}').contains(&c) {
                    chars.next();
                } else {
                    break;
                }
            }
            chars.next();
        }
        // Two-character sequences: ESC 7, ESC c, ESC =, …
        c if ('\u{30}'..='\u{7e}').contains(&c) => {
            chars.next();
        }
        // ESC followed by a control character is malformed; drop only the ESC and let the next
        // pass of the loop decide about the rest.
        _ => {}
    }
}

/// Consume a CSI body: parameter and intermediate bytes, then the final byte.
///
/// A malformed sequence with no final byte stops here rather than eating the rest of the input —
/// losing the tail of a log line to a truncated escape would hide more than it protects.
fn skip_csi_body(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while let Some(&c) = chars.peek() {
        let code = c as u32;
        if (0x20..=0x3f).contains(&code) {
            chars.next();
        } else {
            break;
        }
    }
    if let Some(&c) = chars.peek() {
        if (0x40..=0x7e).contains(&(c as u32)) {
            chars.next();
        }
    }
}

/// Consume an OSC/DCS/APC body up to and including its terminator (BEL, `ESC \` or C1 ST).
///
/// A newline also ends it: no legitimate control string spans lines, and an unterminated OSC must
/// not be able to swallow an entire log buffer.
fn skip_string_body(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while let Some(&c) = chars.peek() {
        if c == '\n' {
            return;
        }
        chars.next();
        match c {
            '\u{7}' | '\u{9c}' => return,
            '\u{1b}' => {
                if chars.peek() == Some(&'\\') {
                    chars.next();
                }
                return;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_is_untouched() {
        let plain = "job.build_and_test succeeded in 4.2s";
        assert_eq!(sanitize_line(plain), plain);
        assert_eq!(sanitize_line("tabs\tsurvive"), "tabs\tsurvive");
        assert_eq!(sanitize_line("unicode ✅ 日本語"), "unicode ✅ 日本語");
    }

    #[test]
    fn sgr_colour_sequences_vanish_and_leave_their_text() {
        assert_eq!(sanitize_line("\u{1b}[31mFAILED\u{1b}[0m"), "FAILED");
        assert_eq!(sanitize_line("\u{1b}[1;38;5;196mred\u{1b}[m"), "red");
    }

    #[test]
    fn screen_clearing_and_cursor_moves_vanish() {
        assert_eq!(sanitize_line("\u{1b}[2J\u{1b}[H owned"), " owned");
        assert_eq!(sanitize_line("a\u{1b}[10;20Hb"), "ab");
        assert_eq!(sanitize_line("\u{1b}[?25lhidden\u{1b}[?25h"), "hidden");
    }

    #[test]
    fn osc_window_title_hijacks_vanish_in_both_terminations() {
        assert_eq!(sanitize_line("\u{1b}]0;pwned\u{7}safe"), "safe");
        assert_eq!(sanitize_line("\u{1b}]0;pwned\u{1b}\\safe"), "safe");
        assert_eq!(sanitize_line("\u{1b}]0;pwned\u{9c}safe"), "safe");
    }

    #[test]
    fn osc_8_hyperlinks_keep_their_label_and_lose_their_target() {
        let hostile = "\u{1b}]8;;https://evil.example/\u{7}click me\u{1b}]8;;\u{7}";
        assert_eq!(sanitize_line(hostile), "click me");
        let st_form = "\u{1b}]8;;https://evil.example/\u{1b}\\click me\u{1b}]8;;\u{1b}\\";
        assert_eq!(sanitize_line(st_form), "click me");
    }

    #[test]
    fn a_bare_escape_is_dropped_without_eating_the_line() {
        assert_eq!(sanitize_line("before\u{1b}"), "before");
        assert_eq!(sanitize_line("\u{1b}"), "");
        // ESC followed by a control char is malformed; only the ESC goes.
        assert_eq!(sanitize_line("a\u{1b}\u{1}b"), "ab");
    }

    #[test]
    fn an_unterminated_control_string_stops_at_the_newline() {
        assert_eq!(sanitize_block("\u{1b}]0;never ends\nkept"), "\nkept");
    }

    #[test]
    fn a_truncated_csi_does_not_swallow_the_rest() {
        // No final byte at all: the parameter bytes go, the words stay.
        assert_eq!(sanitize_line("head\u{1b}[1;2"), "head");
        // ECMA-48 says a space is an intermediate byte, so a malformed sequence legitimately
        // consumes " t" before finding a final byte. What matters is that the line survives.
        assert_eq!(sanitize_line("head\u{1b}[1;2 tail"), "headail");
        assert_eq!(sanitize_line("head\u{1b}[999999999mtail"), "headtail");
    }

    #[test]
    fn eight_bit_c1_introducers_are_stripped_too() {
        assert_eq!(sanitize_line("a\u{9b}31mb"), "ab");
        assert_eq!(sanitize_line("a\u{9d}0;title\u{7}b"), "ab");
        assert_eq!(sanitize_line("a\u{85}b"), "ab");
    }

    #[test]
    fn c0_controls_go_and_tab_stays() {
        assert_eq!(sanitize_line("a\u{0}b\u{7}c\u{8}d\u{7f}e\tf"), "abcde\tf");
    }

    #[test]
    fn carriage_returns_never_rewrite_the_line() {
        assert_eq!(
            sanitize_line("progress 10%\rprogress 100%"),
            "progress 10%progress 100%"
        );
        assert_eq!(sanitize_block("one\r\ntwo\r\n"), "one\ntwo\n");
        assert_eq!(sanitize_block("one\rtwo"), "onetwo");
    }

    #[test]
    fn a_line_cannot_forge_a_second_line() {
        assert_eq!(sanitize_line("real\nforged"), "realforged");
    }

    #[test]
    fn a_block_keeps_its_shape_and_sanitises_every_line() {
        let body = "## Intent\r\n\u{1b}[31mrisk\u{1b}[0m: none\r\n\u{1b}]0;x\u{7}done";
        assert_eq!(sanitize_block(body), "## Intent\nrisk: none\ndone");
    }

    #[test]
    fn bounds_are_enforced_and_marked() {
        assert_eq!(sanitize_line_bounded("abcdef", 3), "abc\u{2026}");
        assert_eq!(sanitize_line_bounded("abc", 3), "abc");
        assert_eq!(sanitize_line_bounded("", 3), "");
        // Escape sequences do not count toward the bound; only visible characters do.
        assert_eq!(sanitize_line_bounded("\u{1b}[31mabc\u{1b}[0m", 3), "abc");
        assert_eq!(sanitize_block_bounded("aaaa\nbbbb", 6), "aaaa\nb\u{2026}");
    }

    #[test]
    fn output_is_never_longer_than_the_input() {
        let hostile = "\u{1b}[2J\u{1b}]0;t\u{7}\u{1b}[31mx\u{1b}[0m\r\n\u{0}\u{7f}";
        assert!(sanitize_block(hostile).len() <= hostile.len());
        assert!(sanitize_line(hostile).len() <= hostile.len());
    }

    #[test]
    fn nothing_a_terminal_acts_on_survives() {
        let hostile = concat!(
            "\u{1b}[2J\u{1b}]0;hijack\u{7}\u{1b}Pq;junk\u{1b}\\",
            "\u{1b}(0\u{1b}7\u{9b}5A visible \u{1b}_apc\u{9c}"
        );
        let clean = sanitize_line(hostile);
        assert_eq!(clean, " visible ");
        assert!(!clean.chars().any(|c| (c as u32) < 0x20 || c == '\u{7f}'));
    }
}
