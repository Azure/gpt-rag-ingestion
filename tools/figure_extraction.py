"""
Format-specific figure extraction utilities for the Content Understanding path.

When using Content Understanding instead of Document Intelligence, the
get_figure API is not available. These helpers extract figure images directly
from the source document bytes using bounding-box cropping for PDFs or embedded
media extraction for Office documents.
"""

import io
import logging
import zipfile


def extract_figure_from_pdf(file_bytes: bytes, figure: dict, dpi: int = 200) -> bytes | None:
    """Extract a figure region from a PDF page."""
    import fitz  # PyMuPDF
    from PIL import Image

    bounding_regions = figure.get("boundingRegions", [])
    if not bounding_regions:
        return None

    region = bounding_regions[0]
    page_number = region.get("pageNumber", 1)
    polygon = region.get("polygon", [])

    if len(polygon) < 4:
        return None

    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        page = doc[page_number - 1]

        page_w = page.rect.width
        page_h = page.rect.height
        max_render_px = 2500
        max_dim_pt = max(page_w, page_h)
        effective_dpi = min(dpi, int(max_render_px * 72 / max_dim_pt))
        effective_dpi = max(effective_dpi, 72)

        zoom = effective_dpi / 72
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        doc.close()

        logging.info(
            f"[figure_extraction] Page {page_number}: rendered "
            f"{pix.width}x{pix.height} at {effective_dpi} dpi "
            f"(page={page_w:.0f}x{page_h:.0f} pt)"
        )

        xs = [polygon[i] * effective_dpi for i in range(0, len(polygon), 2)]
        ys = [polygon[i] * effective_dpi for i in range(1, len(polygon), 2)]
        left = max(0, int(min(xs)))
        top = max(0, int(min(ys)))
        right = min(img.width, int(max(xs)))
        bottom = min(img.height, int(max(ys)))

        if right <= left or bottom <= top:
            logging.warning(
                f"[figure_extraction] Invalid crop box on page {page_number}: "
                f"({left},{top})-({right},{bottom})"
            )
            return None

        cropped = img.crop((left, top, right, bottom))
        logging.info(
            f"[figure_extraction] Cropped figure to "
            f"{cropped.width}x{cropped.height}"
        )

        buf = io.BytesIO()
        cropped.save(buf, format="PNG")
        return buf.getvalue()
    except Exception as e:
        logging.error(f"[figure_extraction] PDF figure crop failed: {e}")
        return None


def _extract_images_from_ooxml(file_bytes: bytes, media_prefix: str) -> list[bytes]:
    """Extract all embedded images from an OOXML ZIP package."""
    images = []
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            media_files = sorted(
                name for name in z.namelist()
                if name.startswith(media_prefix) and not name.endswith("/")
            )
            for name in media_files:
                images.append(z.read(name))
    except Exception as e:
        logging.error(f"[figure_extraction] OOXML image extraction failed: {e}")
    return images


def _extract_all_pdf_images(file_bytes: bytes, dpi: int = 200) -> list[bytes]:
    """Render each PDF page as an image for fallback figure mapping."""
    import fitz  # PyMuPDF
    from PIL import Image

    images: list[bytes] = []
    max_render_px = 2500
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        for page_idx, page in enumerate(doc):
            page_w = page.rect.width
            page_h = page.rect.height
            max_dim_pt = max(page_w, page_h)
            effective_dpi = min(dpi, int(max_render_px * 72 / max_dim_pt))
            effective_dpi = max(effective_dpi, 72)

            zoom = effective_dpi / 72
            mat = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat, alpha=False)

            buf = io.BytesIO()
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            img.save(buf, format="PNG")
            images.append(buf.getvalue())

            logging.info(
                f"[figure_extraction] Fallback: rendered page {page_idx + 1} "
                f"as {pix.width}x{pix.height} at {effective_dpi} dpi"
            )
        doc.close()
    except Exception as e:
        logging.error(f"[figure_extraction] PDF page rendering failed: {e}")
    return images


def build_figure_image_map(
    file_bytes: bytes,
    extension: str,
    figures: list[dict],
) -> dict[str, bytes]:
    """Build a mapping of figure id to image bytes for every figure."""
    image_map: dict[str, bytes] = {}

    if extension == "pdf":
        has_bounds = any(fig.get("boundingRegions") for fig in figures)
        logging.info(
            f"[figure_extraction] PDF path: {len(figures)} figures, "
            f"has_bounds={has_bounds}"
        )
        if has_bounds:
            for fig in figures:
                fig_id = fig.get("id")
                if not fig_id:
                    continue
                image_data = extract_figure_from_pdf(file_bytes, fig)
                if image_data:
                    image_map[fig_id] = image_data
                else:
                    logging.warning(
                        f"[figure_extraction] Could not crop PDF figure {fig_id}"
                    )
        else:
            all_images = _extract_all_pdf_images(file_bytes)
            for fig in figures:
                fig_id = fig.get("id")
                if not fig_id:
                    continue
                try:
                    page_num = int(fig_id.split(".")[0])
                except (ValueError, IndexError):
                    logging.warning(
                        f"[figure_extraction] Cannot parse page from "
                        f"figure id '{fig_id}'; skipping"
                    )
                    continue
                page_idx = page_num - 1
                if 0 <= page_idx < len(all_images):
                    image_map[fig_id] = all_images[page_idx]
                    logging.info(
                        f"[figure_extraction] Mapped figure {fig_id} "
                        f"to page {page_num} render"
                    )
                else:
                    logging.warning(
                        f"[figure_extraction] Figure {fig_id} refers to "
                        f"page {page_num} but PDF has {len(all_images)} pages"
                    )

    elif extension == "docx":
        all_images = _extract_images_from_ooxml(file_bytes, "word/media/")
        for idx, fig in enumerate(figures):
            fig_id = fig.get("id")
            if fig_id and idx < len(all_images):
                image_map[fig_id] = all_images[idx]

    elif extension == "pptx":
        all_images = _extract_images_from_ooxml(file_bytes, "ppt/media/")
        for idx, fig in enumerate(figures):
            fig_id = fig.get("id")
            if fig_id and idx < len(all_images):
                image_map[fig_id] = all_images[idx]

    return image_map
