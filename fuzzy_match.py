"""Fuzzy string matching that does not require a third-party package.

rapidfuzz is used when it is already installed. Otherwise matching uses
difflib from the Python standard library, so a fresh QGIS install can
load the plugin without fuzzywuzzy or rapidfuzz.
"""

import difflib

try:
    from rapidfuzz import fuzz as _rapidfuzz
    from rapidfuzz import process as _rapidfuzz_process
except ImportError:
    _rapidfuzz = None
    _rapidfuzz_process = None


def ratio(left, right):
    """Return a similarity score from 0 to 100."""
    if _rapidfuzz is not None:
        return _rapidfuzz.ratio(left, right)
    if left is None or right is None:
        return 0
    score = difflib.SequenceMatcher(None, str(left), str(right)).ratio()
    return int(round(score * 100))


class _Fuzz:
    ratio = staticmethod(ratio)


fuzz = _Fuzz()


def extract(query, choices, scorer=None, score_cutoff=0):
    """Return (choice, score, index) matches at or above score_cutoff."""
    if _rapidfuzz_process is not None:
        return _rapidfuzz_process.extract(
            query,
            choices,
            scorer=scorer or _rapidfuzz.ratio,
            score_cutoff=score_cutoff,
        )
    scored = []
    for index, choice in enumerate(choices):
        score = (scorer or ratio)(query, choice)
        if score >= score_cutoff:
            scored.append((choice, score, index))
    scored.sort(key=lambda item: item[1], reverse=True)
    return scored


def extractOne(query, choices, scorer=None, score_cutoff=0):
    """Return the best (choice, score, index) match, or None."""
    if _rapidfuzz_process is not None:
        return _rapidfuzz_process.extractOne(
            query,
            choices,
            scorer=scorer or _rapidfuzz.ratio,
            score_cutoff=score_cutoff,
        )
    matches = extract(query, choices, scorer=scorer, score_cutoff=score_cutoff)
    return matches[0] if matches else None


class process:
    extract = staticmethod(extract)
    extractOne = staticmethod(extractOne)
