"""
Converts PDF text content (though not images containing text) to plain text, html, xml or "tags".

Pdfminer is kind of a mess; their API docs are not very clear and setting up a pdf processor requires several steps.
The code below is a slightly modified version of the pdf2txt.py script that ships with the tool, which seems to
capture most of the features we want to extract from bill pdfs. """

from io import StringIO, BytesIO
import logging
import os
from tempfile import NamedTemporaryFile
from typing import List

import pdfminer.high_level
from pdfminer.image import ImageWriter
import pdfminer.layout
import pdfminer.settings
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage


pdfminer.settings.STRICT = False
logging.getLogger("pdfminer").setLevel(logging.WARNING)


def pdf_pages(pdf: BytesIO) -> List[str]:
    pages = []
    rsrcmgr = PDFResourceManager()
    with StringIO() as retstr, TextConverter(
        rsrcmgr, retstr, codec="utf-8", laparams=LAParams()
    ) as device:
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        for page in PDFPage.get_pages(pdf, check_extractable=False):
            interpreter.process_page(page)
            pages.append(retstr.getvalue())
            retstr.truncate(0)
            retstr.seek(0)

    return pages


def extract_text(
    files=[],
    outfile=None,
    _py2_no_more_posargs=None,
    no_laparams=False,
    all_texts=None,
    detect_vertical=None,  # LAParams
    word_margin=None,
    char_margin=None,
    line_margin=None,
    boxes_flow=None,  # LAParams
    output_type="text",
    codec="utf-8",
    strip_control=False,
    maxpages=0,
    page_numbers=None,
    password="",
    scale=1.0,
    rotation=0,
    layoutmode="normal",
    output_dir=None,
    debug=False,
    disable_caching=False,
    **other
):
    if _py2_no_more_posargs is not None:
        raise ValueError("Too many positional arguments passed.")
    if not files:
        raise ValueError("Must provide files to work upon!")

    # If any LAParams group arguments were passed, create an LAParams object and
    # populate with given args. Otherwise, set it to None.
    if not no_laparams:
        laparams = pdfminer.layout.LAParams()
        for param in (
            "all_texts",
            "detect_vertical",
            "word_margin",
            "char_margin",
            "line_margin",
            "boxes_flow",
        ):
            paramv = locals().get(param, None)
            if paramv is not None:
                setattr(laparams, param, paramv)
    else:
        laparams = None

    imagewriter = None
    if output_dir:
        imagewriter = ImageWriter(output_dir)

    if output_type == "text" and outfile != "-":
        for override, alttype in (
            (".htm", "html"),
            (".html", "html"),
            (".xml", "xml"),
            (".tag", "tag"),
        ):
            if outfile.endswith(override):
                output_type = alttype

    with open(outfile, "wb") as outfp:
        for fname in files:
            with open(fname, "rb") as fp:
                pdfminer.high_level.extract_text_to_fp(fp, **locals())
        outfp.flush()


def pdf_to_str(pdf_filename):
    if not os.path.isfile(pdf_filename):
        raise FileNotFoundError

    with NamedTemporaryFile(mode="wb") as ntf:
        # Write PDF data into the temporary file.
        extract_text(files=[pdf_filename], outfile=ntf.name)

        with open(ntf.name, "r") as f:
            return f.read()


def pdf_bytes_to_str(pdf_byte_stream: bytes):
    with NamedTemporaryFile(mode="wb") as ntf:
        ntf.write(pdf_byte_stream)
        return pdf_to_str(ntf.name)
