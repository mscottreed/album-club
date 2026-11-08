"""Split a OneNote section exported as a Single File Web Page (.mht) into its pages.

Windows OneNote's File > Export > Section > Single File Web Page writes one MIME archive:
an HTML document plus every image on every page. In the HTML, each OneNote page is a
top-level <div> whose header holds the page title, then the created date and time.
"""

import email
from email import policy

from bs4 import BeautifulSoup

# The style OneNote puts on each top-level page <div>. The trailing "Created with OneNote."
# footer is also a top-level <div>, but without this style.
PAGE_DIV_STYLE = "direction:ltr;border-width:100%"


def split_mht(content: bytes) -> list[dict]:
    """Return one dict per page: page_number (1-based), title, created (as OneNote wrote it), html."""
    message = email.message_from_bytes(content, policy=policy.default)
    html = next(part for part in message.walk() if part.get_content_type() == "text/html").get_content()
    soup = BeautifulSoup(html, "html.parser")

    pages = []
    for div in soup.body.find_all("div", recursive=False):
        if div.get("style") != PAGE_DIV_STYLE:
            continue
        title_div, created_div = div.div.find_all("div", recursive=False)[:2]
        pages.append(
            {
                "page_number": len(pages) + 1,
                "title": _text(title_div),
                "created": _text(created_div),
                "html": str(div),
            }
        )
    return pages


def _text(element) -> str:
    """Visible text with whitespace collapsed. OneNote wraps long lines in its HTML source."""
    return " ".join(element.get_text(" ").split())
