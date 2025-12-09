from PyPDF2 import PdfReader, PdfWriter

def process_pdf_pages(input_path, output_path, page_fn):
    """
    input_path:  path to original multipage PDF
    output_path: path to final processed multipage PDF
    page_fn:     function(page, page_index) -> processed_page
    """
    reader = PdfReader(input_path)
    writer = PdfWriter()

    for i, page in enumerate(reader.pages):
        processed_page = page_fn(page, i)
        writer.add_page(processed_page)

    with open(output_path, "wb") as f:
        writer.write(f)


# ---- Example: simple operation on each page ----

def example_page_op(page, idx):
    """
    Example: rotate odd-numbered pages by 90 degrees.
    Replace this with your real logic.
    """
    if idx % 2 == 1:  # 0-based indexing: pages 2,4,6,...
        page.rotate(90)  # in-place, returns None in recent PyPDF2
    return page

# Usage
process_pdf_pages(
    input_path="input_multipage.pdf",
    output_path="output_processed.pdf",
    page_fn=example_page_op,
)
