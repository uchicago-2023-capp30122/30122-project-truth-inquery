import pytest
import re
from truth_inquery.crawler.crawler import PATTERN

# Test regex to match all numbers and special characters except apostrophe/whitespace
@pytest.mark.parametrize("input, output", [
    ("hello!!!", "hello"),
    ("women's", "women's"),
    (".][!@#$%^&*()_+=\"/\\", ""),
    ("45!<words>", "words"),
    ("what 123,", "what "),
])

def test_regex(input, output):
    assert re.sub(PATTERN, "", input) == output
