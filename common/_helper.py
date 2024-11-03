def bold(text):
    """Wrap the text in HTML bold tags."""
    return f"<b>{text}</b>"

def underline(text):
    """Wrap the text in HTML underline tags."""
    return f"<u>{text}</u>"

def italic(text):
    """Wrap the text in HTML italic tags."""
    return f"<i>{text}</i>"

def H1(text):
    """Wrap the text in HTML H1 tags."""
    return f"<h1>{text}</h1>"

def H2(text):
    """Wrap the text in HTML H2 tags."""
    return f"<h2>{text}</h2>"

def H3(text):
    """Wrap the text in HTML H3 tags."""
    return f"<h3>{text}</h3>"

def paragraph(text):
    """Wrap the text in HTML paragraph tags."""
    return f"<p>{text}</p>"

def link(url, text):
    """Wrap the text as a hyperlink."""
    return f'<a href="{url}">{text}</a>'

def line_break():
    """Insert a line break."""
    return "<br>"

def horizontal_rule():
    """Insert a horizontal rule."""
    return "<hr>"

def format_html(body):
    # Function to format HTML content
    return f"<html><body>{body}</body></html>"