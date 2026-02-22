"""Determine whether a dataset's license allows harvesting."""

import re

# Prefixes / identifiers that we consider open enough to collect.
_ACCEPTED = {
    "cc-by",
    "cc-by-sa",
    "cc-by-nc",
    "cc-by-nc-sa",
    "cc-by-nd",
    "cc-by-nc-nd",
    "cc0",
    "cc0-1.0",
    "public domain",
    "odc-by",
    "odc-odbl",
    "odc-pddl",
    "mit",
    "apache-2.0",
    "standard-access",     # QDR convention: docs CC BY-SA 4.0, data open for registered users
    "etalab",              # French open government license (Licence Ouverte)
}

# Phrases found *within* long license text that indicate an open license.
# Checked after stripping HTML and lowercasing.
_OPEN_PHRASES = [
    "creative commons",
    "cc by",
    "cc0",
    "statistics canada open licence",
    "licence ouverte",
    # LOC / US government works
    "public domain",
    "not aware of any copyright",
    "no known restrictions",
    "no known copyright restrictions",
    "non-restricted",
    "fully open content",
    "united states government work",
    # FSD Finland access levels
    "(a) openly available",
    "(a) vapaasti",
]


def license_is_open(identifier: str | None) -> bool:
    """Check whether *identifier* matches any known open-license prefix.

    Handles both short identifiers ("CC BY 4.0") and long license text
    blocks that some Dataverse installations store in termsOfAccess.
    """
    if not identifier:
        return False

    # Strip HTML tags and collapse whitespace for long termsOfAccess blobs
    cleaned = re.sub(r"<[^>]+>", " ", identifier)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    canonical = cleaned.lower().replace(" ", "-").replace("_", "-")

    # Direct prefix match (works for short identifiers)
    if any(canonical.startswith(prefix) for prefix in _ACCEPTED):
        return True

    # Substring search for open-license phrases buried in long text
    lower = cleaned.lower()
    return any(phrase in lower for phrase in _OPEN_PHRASES)
