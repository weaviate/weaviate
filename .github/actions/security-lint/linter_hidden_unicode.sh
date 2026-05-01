#!/usr/bin/env bash
# Lint for hidden/invisible Unicode characters in diffs (trojan-source attack prevention).
# Requires Perl (pre-installed on GitHub Actions Ubuntu runners).
#
# Usage:
#   bash linter_hidden_unicode.sh --stdin      # read diff from stdin (CI mode)
#   bash linter_hidden_unicode.sh <base-ref>   # diff against a base ref
#   bash linter_hidden_unicode.sh              # diff staged changes (git diff --cached)

set -euo pipefail

# Binary file extensions to skip (union of both prior lists).
BINARY_PATTERN='\.(png|jpe?g|gif|ico|bmp|webp|svg|pdf|zip|tar|gz|bz2|xz|7z|rar|jar|war|whl|nupkg|snupkg|exe|dll|so|dylib|a|o|obj|pyc|pyo|class|woff2?|ttf|otf|eot|pb|bin|dat|db|sqlite|wasm|avro|parquet)$'

get_diff() {
    if [[ "${1:-}" == "--stdin" ]]; then
        cat
    elif [[ -n "${1:-}" ]]; then
        # Validate ref argument to prevent command injection
        if ! [[ "$1" =~ ^[a-zA-Z0-9._/-]+$ ]]; then
            echo "ERROR: Invalid ref argument: $1" >&2
            exit 1
        fi
        if ! git rev-parse --verify "$1" >/dev/null 2>&1; then
            echo "ERROR: Git ref not found: $1" >&2
            exit 2
        fi
        git diff "$1"
    else
        git diff --cached
    fi
}

# Perl script that:
# 1. Tracks current file from diff headers
# 2. Skips binary files
# 3. Scans only added lines (starting with +, excluding +++ headers)
# 4. Detects ~30+ categories of invisible/suspicious Unicode characters
PERL_SCRIPT='
use utf8;
use strict;
use warnings;

# Human-readable labels for the codepoints we flag. Anything in the
# character class but not here falls back to describe()s generic label.
my %names = (
    0x00AD => "U+00AD SOFT HYPHEN",
    0x034F => "U+034F COMBINING GRAPHEME JOINER",
    0x061C => "U+061C ARABIC LETTER MARK (bidi)",
    0x115F => "U+115F HANGUL CHOSEONG FILLER",
    0x1160 => "U+1160 HANGUL JUNGSEONG FILLER",
    0x180E => "U+180E MONGOLIAN VOWEL SEPARATOR",
    0x200B => "U+200B ZERO WIDTH SPACE",
    0x200C => "U+200C ZERO WIDTH NON-JOINER",
    0x200D => "U+200D ZERO WIDTH JOINER",
    0x200E => "U+200E LEFT-TO-RIGHT MARK (bidi)",
    0x200F => "U+200F RIGHT-TO-LEFT MARK (bidi)",
    0x2028 => "U+2028 LINE SEPARATOR",
    0x2029 => "U+2029 PARAGRAPH SEPARATOR",
    0x202A => "U+202A LEFT-TO-RIGHT EMBEDDING (bidi)",
    0x202B => "U+202B RIGHT-TO-LEFT EMBEDDING (bidi)",
    0x202C => "U+202C POP DIRECTIONAL FORMATTING (bidi)",
    0x202D => "U+202D LEFT-TO-RIGHT OVERRIDE (bidi)",
    0x202E => "U+202E RIGHT-TO-LEFT OVERRIDE (bidi)",
    0x2060 => "U+2060 WORD JOINER",
    0x2061 => "U+2061 FUNCTION APPLICATION",
    0x2062 => "U+2062 INVISIBLE TIMES",
    0x2063 => "U+2063 INVISIBLE SEPARATOR",
    0x2064 => "U+2064 INVISIBLE PLUS",
    0x2066 => "U+2066 LEFT-TO-RIGHT ISOLATE (bidi)",
    0x2067 => "U+2067 RIGHT-TO-LEFT ISOLATE (bidi)",
    0x2068 => "U+2068 FIRST STRONG ISOLATE (bidi)",
    0x2069 => "U+2069 POP DIRECTIONAL ISOLATE (bidi)",
    0x206A => "U+206A INHIBIT SYMMETRIC SWAPPING (deprecated)",
    0x206B => "U+206B ACTIVATE SYMMETRIC SWAPPING (deprecated)",
    0x206C => "U+206C INHIBIT ARABIC FORM SHAPING (deprecated)",
    0x206D => "U+206D ACTIVATE ARABIC FORM SHAPING (deprecated)",
    0x206E => "U+206E NATIONAL DIGIT SHAPES (deprecated)",
    0x206F => "U+206F NOMINAL DIGIT SHAPES (deprecated)",
    0x3164 => "U+3164 HANGUL FILLER",
    0xFEFF => "U+FEFF BYTE ORDER MARK (mid-line)",
    0xFFA0 => "U+FFA0 HALFWIDTH HANGUL FILLER",
    0xFFF9 => "U+FFF9 INTERLINEAR ANNOTATION ANCHOR",
    0xFFFA => "U+FFFA INTERLINEAR ANNOTATION SEPARATOR",
    0xFFFB => "U+FFFB INTERLINEAR ANNOTATION TERMINATOR",
);

sub describe {
    my ($cp) = @_;
    return $names{$cp} if exists $names{$cp};
    if ($cp == 0xE0001 || ($cp >= 0xE0020 && $cp <= 0xE007F)) {
        return sprintf("U+%04X UNICODE TAG", $cp);
    }
    return sprintf("U+%04X suspicious invisible character", $cp);
}

# Escape strings for GitHub Actions workflow commands.
sub escape_property {
    my ($s) = @_;
    $s =~ s/%/%25/g;
    $s =~ s/\r/%0D/g;
    $s =~ s/\n/%0A/g;
    $s =~ s/:/%3A/g;
    $s =~ s/,/%2C/g;
    return $s;
}

sub escape_message {
    my ($s) = @_;
    $s =~ s/%/%25/g;
    $s =~ s/\r/%0D/g;
    $s =~ s/\n/%0A/g;
    return $s;
}

my $file = "";
my $line_in_file = 0;
my $errors = 0;
my $in_binary = 0;
my $binary_pattern = qr/'"$BINARY_PATTERN"'/i;
my $github_actions = defined $ENV{GITHUB_ACTIONS};

# Suspicious invisible codepoints:
# - Soft hyphen / combining grapheme joiner (U+00AD, U+034F)
# - Bidi marks, embeddings, overrides, isolates (U+061C, U+200E-200F, U+202A-202E, U+2066-2069)
# - Line/paragraph separators (U+2028-2029)
# - Zero-width chars and word joiner (U+200B-200D, U+2060)
# - Invisible math operators (U+2061-2064)
# - Deprecated format chars (U+206A-206F)
# - Hangul fillers (U+115F, U+1160, U+3164, U+FFA0)
# - Mongolian vowel separator (U+180E)
# - BOM mid-line (U+FEFF)
# - Interlinear annotation (U+FFF9-FFFB)
# - Unicode tag block (U+E0001-E007F)
# Variation selectors (U+FE00-FE0F) are intentionally NOT scanned --
# they appear in legitimate emoji sequences (e.g. U+2764 + U+FE0F = "heart").
my $pattern = qr/([\x{00AD}\x{034F}\x{061C}\x{115F}\x{1160}\x{180E}\x{200B}-\x{200F}\x{2028}\x{2029}\x{202A}-\x{202E}\x{2060}-\x{2064}\x{2066}-\x{2069}\x{206A}-\x{206F}\x{3164}\x{FEFF}\x{FFA0}\x{FFF9}-\x{FFFB}\x{E0001}-\x{E007F}])/;

while (<STDIN>) {
    chomp;

    # Track current file from diff headers.
    if (/^\+\+\+ b\/(.+)$/) {
        $file = $1;
        $line_in_file = 0;
        $in_binary = ($file =~ $binary_pattern) ? 1 : 0;
        next;
    }

    # Skip binary file markers.
    if (/^Binary files/) {
        $in_binary = 1;
        next;
    }

    # Hunk header resets the line counter.
    if (/^@@ -\d+(?:,\d+)? \+(\d+)/) {
        $line_in_file = $1 - 1;
        next;
    }

    # Advance the line counter for added and context lines.
    if (/^\+/ || /^ /) {
        $line_in_file++;
    }

    # Only scan added lines, skip binary files
    next if $in_binary;
    next unless /^\+/;
    next if /^\+\+\+ (?:$|b\/|\/dev\/null)/;

    # Remove the leading + for scanning
    my $content = substr($_, 1);

    # Per-line dedup: many zero-width joiners on one line -> one message.
    my %seen;
    while ($content =~ /$pattern/g) {
        my $char = $1;
        my $cp = ord($char);
        next if $seen{$cp}++;
        my $desc = describe($cp);
        my $col = $-[1] + 1;

        # Plain line always prints (job log readability).
        print "ERROR: $file:$line_in_file:$col - $desc\n";
        # GitHub Actions annotation (surfaces in the PR "Files changed" view).
        if ($github_actions) {
            my $efile = escape_property($file);
            my $emsg = escape_message("Hidden Unicode character $desc");
            print "::error file=${efile},line=${line_in_file},col=${col}::${emsg}\n";
        }
        $errors++;
    }
}

if ($errors > 0) {
    print "\nFound $errors hidden Unicode character(s) in added lines.\n";
    print "These may indicate a trojan-source attack. See https://trojansource.codes/\n";
    exit 1;
}

print "No hidden Unicode characters detected.\n";
exit 0;
'

get_diff "$@" | perl -CSD -e "$PERL_SCRIPT"
