#!/usr/bin/env python3
"""
xtractr — Unified CLI entry point.

Usage:
    xtractr clip <url-or-file> [--output-dir DIR] [--to refs|papers|datasheets]
    xtractr tweets <subcommand> [options]
    xtractr extract [--dry-run] [--skip-replies]
"""

import sys


def main():
    if len(sys.argv) < 2:
        print("Usage: xtractr <command> [args...]")
        print()
        print("Commands:")
        print("  clip      Clip web articles, PDFs, and arxiv papers")
        print("  tweets    Twitter/X bookmark sync, export, and TUI")
        print("  extract   Resolve and clip links found in tweets")
        sys.exit(1)

    command = sys.argv[1]
    # Remove the command name so submodule parsers see the right argv
    sys.argv = [f"xtractr {command}"] + sys.argv[2:]

    if command == "clip":
        from .clip import main as clip_main
        clip_main()
    elif command == "tweets":
        from .tweets import main as tweets_main
        tweets_main()
    elif command == "extract":
        from .extract import main as extract_main
        extract_main()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
