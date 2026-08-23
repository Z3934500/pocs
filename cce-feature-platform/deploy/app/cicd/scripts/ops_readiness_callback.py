from __future__ import annotations

import argparse
import re


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate operational release readiness.")
    parser.add_argument("--environment", required=True, choices=["dev", "staging", "production"])
    parser.add_argument("--image-tag", required=True)
    parser.add_argument("--require-immutable-tag", action="store_true")
    args = parser.parse_args()

    if args.require_immutable_tag and args.image_tag in {"latest", "dev", "staging", "production"}:
        raise SystemExit(f"Mutable image tag is not allowed for this release: {args.image_tag}")

    if args.environment == "production" and not re.match(r"^v\d+\.\d+\.\d+$", args.image_tag):
        raise SystemExit("Production image tag must look like v1.2.3")

    print("Ops readiness callback passed.")
    print(f"environment={args.environment}")
    print(f"image_tag={args.image_tag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
