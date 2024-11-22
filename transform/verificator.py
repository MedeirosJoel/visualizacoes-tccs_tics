from typing import Literal

from langdetect import detect

def text_is_written_in_language(text: str, language: Literal['pt', 'en']) -> bool:
    language_recognise = detect(text)
    if language_recognise == language:
        return True
    else:
        return False
