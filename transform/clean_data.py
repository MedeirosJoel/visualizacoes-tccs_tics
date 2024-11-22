import re
from typing import Optional


def obtain_first_number_in_text(text: str) -> Optional[int]:
    text = str(text)
    if len(text) == 200:
        return None
    text = str(text)
    search_result = re.search(r'\d+', text)
    result = search_result.group() if search_result else None

    return result
