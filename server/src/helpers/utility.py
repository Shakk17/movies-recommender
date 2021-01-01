import datetime
import html
import re


def strip_html_tags(text):
    """
    This method takes a string of text, unescapes special characters and removes any HTML tag from it.
    :param text: A string of text.
    :return: A string without escaped characters or HTML tags.
    """
    # Unescape difficult character like &amp;.
    text = html.unescape(str(text))
    # Remove all the html tags.
    regex = re.compile(r'<[^>]+>')
    text = regex.sub('', text)
    # Remove \n and \t.
    text = text.replace("\n", " ")
    text = text.replace("\t", " ")
    # Remove spaces at the beginning and at the end of the string.
    text = text.strip()
    return text


def add_scheme(url):
    """
    This method takes a URL and returns a well-formed URL. If the schema is missing, it will get added.
    :param url: A string containing a URL.
    :return: A string containing a well-formed URL.
    """
    url = url.replace("www.", "")
    if not re.match('(?:http|ftp|https)://', url):
        return 'http://{}'.format(url)
    return url


def remove_scheme(url):
    # Remove www.
    url = url.replace("www.", "")
    # Remove scheme.
    try:
        idx = url.index("://")
        url = url[idx + 3:]
    except ValueError:
        pass
    # Remove trailing / at the end of homepages.
    if url.endswith('/') and '.' in url[-5:]:
        url = url[:-1]

    return url


def extract_words(string):
    """
    Given a string, it returns all the words contained in it.
    :param string: The string to be analysed.
    :return: A list containing all the words contained in the string.
    """
    regex = r'\b\w+\b'
    words = re.findall(regex, string)
    return words


def get_time():
    """
    This method returns a string containing the current time.
    :return: String in format HH:MM:SS.
    """
    time = datetime.datetime.now().time()
    string = time.strftime("%H:%M:%S")
    return string


def transform_length(time):
    minutes = 0
    if 'h' in time:
        minutes += int(time.split("h")[0]) * 60
        time = time.split('h')[1]
    if "min" in time:
        minutes += int(time.split("min")[0])
    return minutes
