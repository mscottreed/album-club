from album_club.onenote_export import split_mht

PAGE = """<div style="direction:ltr;border-width:100%">
<div style="direction:ltr;margin-top:0in;margin-left:0in;width:7.7652in">
<div style="direction:ltr;margin-top:0in;margin-left:.2652in;width:6.9784in">
<p style='margin:0in;font-family:"Calibri Light";font-size:20.0pt'>{title}</p>
</div>
<div style="direction:ltr;margin-top:.0444in;margin-left:.2652in;width:2.5666in">
<p style="margin:0in;font-family:Calibri;font-size:10.0pt;color:#767676">{date}</p>
<p style="margin:0in;font-family:Calibri;font-size:10.0pt;color:#767676">{time}</p>
</div>
<div><table><tr><td>Name</td><td>Rating</td></tr><tr><td>Alex</td><td>7</td></tr></table></div>
</div>
</div>"""

FOOTER = """<div><p style="margin:0in"> </p><p>Created with OneNote.</p></div>"""


def make_mht(*pages: str) -> bytes:
    """Build a minimal .mht shaped like OneNote's export: one HTML part plus an image part."""
    html = "<html><body>" + "<p>&nbsp;</p>".join(pages) + FOOTER + "</body></html>"
    return (
        'MIME-Version: 1.0\nContent-Type: multipart/related; boundary="b"\n\n'
        '--b\nContent-Location: file:///C:/x/Music.htm\nContent-Type: text/html; charset="utf-8"\n\n'
        f"{html}\n"
        "--b\nContent-Location: file:///C:/x/image001.png\nContent-Type: image/png\n"
        "Content-Transfer-Encoding: base64\n\niVBORw0KGgo=\n"
        "--b--\n"
    ).encode()


def test_splits_each_page_and_skips_footer():
    content = make_mht(
        PAGE.format(
            title="W001 - #1 - Frank Sinatra - In the Wee Small Hours (1955)",
            date="Sunday, November 21, 2021",
            time="11:08 AM",
        ),
        PAGE.format(
            title="W002 - #201 - The Stooges - Fun House (1970)", date="Thursday, December 16, 2021", time="4:07 PM"
        ),
    )

    pages = split_mht(content)

    assert [p["page_number"] for p in pages] == [1, 2]
    assert pages[0]["title"] == "W001 - #1 - Frank Sinatra - In the Wee Small Hours (1955)"
    assert pages[0]["created"] == "Sunday, November 21, 2021 11:08 AM"
    assert pages[1]["created"] == "Thursday, December 16, 2021 4:07 PM"


def test_keeps_page_html_including_tables():
    content = make_mht(PAGE.format(title="W001", date="Sunday, November 21, 2021", time="11:08 AM"))

    html = split_mht(content)[0]["html"]

    assert html.startswith('<div style="direction:ltr;border-width:100%">')
    assert "<td>Rating</td>" in html


def test_collapses_line_breaks_onenote_puts_in_long_lines():
    content = make_mht(
        PAGE.format(title="Albums\r\n(Crafting Bytes)", date="Monday,\r\nAugust 15, 2016", time="4:20\r\nPM")
    )

    page = split_mht(content)[0]

    assert page["title"] == "Albums (Crafting Bytes)"
    assert page["created"] == "Monday, August 15, 2016 4:20 PM"
