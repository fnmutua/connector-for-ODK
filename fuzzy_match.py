"""Field matching through rapidfuzz.

rapidfuzz is what the import dialog used for fuzzy scores. If it is not
installed, matching falls back to difflib so the plugin can still load.
"""

import difflib
import re

try:
    from rapidfuzz import fuzz as _rapidfuzz
    from rapidfuzz import process as _rapidfuzz_process
    from rapidfuzz import utils as _rapidfuzz_utils
except ImportError:
    _rapidfuzz = None
    _rapidfuzz_process = None
    _rapidfuzz_utils = None


def _prepare(value):
    if value is None:
        return ""
    text = re.sub(r"[^a-z0-9]+", " ", str(value).lower())
    return " ".join(text.split())


def _fallback_ratio(left, right):
    left_text = _prepare(left)
    right_text = _prepare(right)
    if not left_text and not right_text:
        return 100
    if not left_text or not right_text:
        return 0
    score = difflib.SequenceMatcher(None, left_text, right_text).ratio()
    return int(round(score * 100))


class _Fuzz:
    if _rapidfuzz is not None:
        ratio = staticmethod(_rapidfuzz.ratio)
        partial_ratio = staticmethod(_rapidfuzz.partial_ratio)
        WRatio = staticmethod(_rapidfuzz.WRatio)
    else:
        ratio = staticmethod(_fallback_ratio)
        partial_ratio = staticmethod(_fallback_ratio)
        WRatio = staticmethod(_fallback_ratio)


fuzz = _Fuzz()


def _processor():
    if _rapidfuzz_utils is not None:
        return _rapidfuzz_utils.default_process
    return _prepare


def extract(query, choices, scorer=None, score_cutoff=0, processor=None):
    """Return (choice, score, index) matches at or above score_cutoff."""
    scorer_fn = scorer or fuzz.ratio
    processor_fn = _processor() if processor is None else processor
    if _rapidfuzz_process is not None:
        return _rapidfuzz_process.extract(
            query,
            choices,
            scorer=scorer_fn,
            processor=processor_fn,
            score_cutoff=score_cutoff,
        )
    prepared_query = processor_fn(query)
    scored = []
    for index, choice in enumerate(choices):
        score = scorer_fn(prepared_query, processor_fn(choice))
        if score >= score_cutoff:
            scored.append((choice, score, index))
    scored.sort(key=lambda item: item[1], reverse=True)
    return scored


def extractOne(query, choices, scorer=None, score_cutoff=0, processor=None):
    """Return the best (choice, score, index) match, or None."""
    if _rapidfuzz_process is not None:
        return _rapidfuzz_process.extractOne(
            query,
            choices,
            scorer=scorer or fuzz.ratio,
            processor=_processor() if processor is None else processor,
            score_cutoff=score_cutoff,
        )
    matches = extract(
        query,
        choices,
        scorer=scorer,
        score_cutoff=score_cutoff,
        processor=processor,
    )
    return matches[0] if matches else None


class process:
    extract = staticmethod(extract)
    extractOne = staticmethod(extractOne)
