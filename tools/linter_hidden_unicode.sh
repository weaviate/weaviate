#!/usr/bin/env bash

set -euo pipefail

# Detect hidden/invisible Unicode characters in added or modified lines.
# These characters are invisible in GitHub's diff view and can be used for
# trojan-source attacks (bidi overrides), identifier confusion (zero-width
# chars), or parser manipulation (mid-file BOMs).
#
# By default, scans staged changes (git diff --cached). Pass a base ref
# (e.g., "main") to scan all changes since that ref instead.
#
# Usage:
#   ./tools/linter_hidden_unicode.sh          # scan staged changes
#   ./tools/linter_hidden_unicode.sh main     # scan changes since main
#   ./tools/linter_hidden_unicode.sh --stdin  # read diff from stdin (for CI piping)

ARG="${1:-}"

if [ "$ARG" = "--stdin" ]; then
    # Read diff from stdin — used by pr-security-lint.yaml to pipe the
    # GitHub API diff directly without checking out PR code.
    diff_cmd=(cat)
elif [ -n "$ARG" ]; then
    # Validate that ARG looks like a git ref (alphanumeric, -, _, /, .)
    # to prevent command injection via crafted arguments.
    if ! [[ "$ARG" =~ ^[a-zA-Z0-9._/-]+$ ]]; then
        echo "Error: invalid git ref: $ARG" >&2
        exit 1
    fi
    diff_cmd=(git diff "$ARG...HEAD")
else
    diff_cmd=(git diff --cached)
fi

# Verify the git ref exists before piping to perl, so failures produce a
# clear error instead of a silent non-zero exit from pipefail.
if [ "${diff_cmd[0]}" = "git" ]; then
    if ! git rev-parse --verify "$ARG" >/dev/null 2>&1; then
        echo "Error: git ref not found: $ARG" >&2
        exit 2
    fi
fi

# Use perl for portable Unicode-aware matching. The script parses the unified
# diff, tracks file names and line numbers, and checks each added line for
# invisible characters.
"${diff_cmd[@]}" | perl -CSD -e '
use strict;
use warnings;
use utf8;

# Map codepoints to human-readable descriptions.
my %names = (
    0x00AD => "U+00AD SOFT HYPHEN",
    0x061C => "U+061C ARABIC LETTER MARK",
    0x180E => "U+180E MONGOLIAN VOWEL SEPARATOR",
    0x200B => "U+200B ZERO WIDTH SPACE",
    0x200C => "U+200C ZERO WIDTH NON-JOINER",
    0x200D => "U+200D ZERO WIDTH JOINER",
    0x200E => "U+200E LEFT-TO-RIGHT MARK (bidi)",
    0x200F => "U+200F RIGHT-TO-LEFT MARK (bidi)",
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
    0x2028 => "U+2028 LINE SEPARATOR",
    0x2029 => "U+2029 PARAGRAPH SEPARATOR",
    0x115F => "U+115F HANGUL CHOSEONG FILLER",
    0x1160 => "U+1160 HANGUL JUNGSEONG FILLER",
    0x3164 => "U+3164 HANGUL FILLER",
    0xFEFF => "U+FEFF BYTE ORDER MARK (mid-line)",
    0xFFA0 => "U+FFA0 HALFWIDTH HANGUL FILLER",
);

# Regex character class covering all suspicious codepoints.
# Bidi:        U+200E-200F, U+202A-202E, U+2066-2069
# Zero-width:  U+200B-200D, U+2060
# Invisible:   U+00AD, U+061C, U+180E, U+2061-2064, U+FEFF
# Separators:  U+2028-2029
# Hangul fill: U+115F, U+1160, U+3164, U+FFA0
# Tag block:   U+E0001-E007F
my $pattern = qr/[\x{00AD}\x{061C}\x{115F}\x{1160}\x{180E}\x{200B}-\x{200F}\x{202A}-\x{202E}\x{2028}\x{2029}\x{2060}-\x{2064}\x{2066}-\x{2069}\x{3164}\x{FEFF}\x{FFA0}\x{E0001}-\x{E007F}]/;

my $file = "";
my $lineno = 0;
my $found = 0;
my $skip_file = 0;

# Detect GitHub Actions environment for annotations.
my $github_actions = defined $ENV{GITHUB_ACTIONS};

# Escape strings for GitHub Actions workflow commands.
# Properties (file, line) must escape %, \r, \n, :, and ,.
# The message part must escape %, \r, and \n.
sub escape_property {
    my $s = shift;
    $s =~ s/%/%25/g;
    $s =~ s/\r/%0D/g;
    $s =~ s/\n/%0A/g;
    $s =~ s/:/%3A/g;
    $s =~ s/,/%2C/g;
    return $s;
}
sub escape_message {
    my $s = shift;
    $s =~ s/%/%25/g;
    $s =~ s/\r/%0D/g;
    $s =~ s/\n/%0A/g;
    return $s;
}

while (my $line = <STDIN>) {
    chomp $line;

    # Track current file.
    if ($line =~ /^diff --git a\/.* b\/(.*)/) {
        $file = $1;
        # Skip binary files (images, archives, compiled output, fonts, etc.).
        $skip_file = ($file =~ /\.(?:png|jpe?g|gif|ico|bmp|webp|svg|pdf|zip|tar|gz|bz2|xz|jar|war|whl|exe|dll|so|dylib|a|o|pyc|class|woff2?|ttf|otf|eot|pb|bin|dat|db|sqlite|wasm|avro|parquet)$/i) ? 1 : 0;
        next;
    }

    # Skip "Binary files ... differ" markers.
    if ($line =~ /^Binary files .* differ$/) {
        $skip_file = 1;
        next;
    }

    next if $skip_file;

    # Track line number from hunk header.
    if ($line =~ /^\@\@ -\d+(?:,\d+)? \+(\d+)/) {
        $lineno = $1;
        next;
    }

    # Added lines only (exclude diff headers like "+++ b/..." or "+++ /dev/null").
    if ($line =~ /^\+/ && $line !~ /^\+\+\+ (?:$|b\/|\/dev\/null)/) {
        my $content = substr($line, 1);

        # Find all suspicious characters in the line.
        my %seen;
        while ($content =~ /($pattern)/g) {
            my $cp = ord($1);
            next if $seen{$cp}++;
            my $desc = $names{$cp} // sprintf("U+%04X suspicious invisible character", $cp);
            print "Error: $file:$lineno: hidden character found: $desc\n";
            if ($github_actions) {
                my $efile = escape_property($file);
                my $emsg = escape_message("Hidden character found: $desc");
                print "::error file=$efile,line=$lineno" . "::$emsg\n";
            }
            $found = 1;
        }

        $lineno++;
    } elsif ($line !~ /^-/ && $line !~ /^\\/) {
        # Context line — increment line counter.
        $lineno++;
    }
    # Deleted lines: do not increment lineno.
}

if ($found) {
    print "\nHidden Unicode characters were found in added lines.\n";
    print "These characters are invisible in GitHub'\''s diff view and can be used\n";
    print "for trojan-source attacks or to confuse code reviewers (human and LLM).\n";
    print "Remove them or replace with visible ASCII equivalents.\n";
    exit 1;
}

print "No hidden Unicode characters found.\n";
'
