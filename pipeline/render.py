# pylint: disable=C0103,C0301,E0401
"""Render a jupyter notebook to html,
designed for notebooks in ancillary/"""

import argparse
import subprocess

def main():
    """Render a jupyter notebook to html"""
    parser = argparse.ArgumentParser(
        description="Render a .ipynb to html using nbconvert (for project updates)"
    )
    parser.add_argument(
        "-f",
        "--file-path",
        action="store",
        dest="fp",
        type=str,
        help=("Path to file to be converted"),
    )
    parser.add_argument(
        "-o",
        "--out-dir",
        action="store",
        dest="out_dir",
        type=str,
        help=("Directory to save converted nb"),
    )

    # unpack args
    args = parser.parse_args()
    fp = args.fp
    out_dir = args.out_dir

    command = [
        "jupyter",
        "nbconvert",
        fp,
        "--output-dir",
        out_dir,
        "--template",
        "classic",
        "--to",
        "html",
        "--no-prompt",
        "--no-input",
        "--execute",
    ]
    subprocess.Popen(command)

if __name__ == "__main__":
    main()
