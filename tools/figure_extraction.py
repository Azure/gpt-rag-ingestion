"""
Format-specific figure extraction utilities for the Content Understanding path.

When using Content Understanding (instead of Document Intelligence), the
get_figure API is not available.  These helpers extract figure images directly
from the source document bytes using bounding-box cropping (PDF) or embedded
media extraction (DOCX / PPTX).
"""

import io
import logging
import zipfile


def _crop_pil_image_to_bbox(img, polygon_inches, page_width_pt, page_height_pt):
    """Crop a PIL Image using a bounding-box polygon expressed in inches.

    The embedded image is assumed to cover the full page, so we scale the
    inch-based polygon coordinates to image pixel coordinates using the ratio
    between the image dimensions and the page dimensions (in inches,
    derived from points: 1 inch = 72 pt).
    """
    from PIL import Image

    page_width_in = page_width_pt / 72.0
    page_height_in = page_height_pt / 72.0

    ppi_x = img.width / page_width_in
    ppi_y = img.height / page_height_in

    xs = [polygon_inches[i] * ppi_x for i in range(0, len(polygon_inches), 2)]
    ys = [polygon_inches[i] * ppi_y for i in range(1, len(polygon_inches), 2)]

    left = max(0, int(min(xs)))
    top = max(0, int(min(ys)))
    right = min(img.width, int(max(xs)))
    bottom = min(img.height, int(max(ys)))

    if right <= left or bottom <= top:
        return None

    return img.crop((left, top, right, bottom))


def _image_quality_score(img):
    """Return a quality score for a PIL Image (higher = better for documents).

    Uses mean brightness as the primary indicator: properly scanned
    documents have a bright (white) background, while corrupted/artifact-
    laden images tend to be very dark.
    """
    gray = img.convert("L")
    pixels = list(gray.getdata())
    mean = sum(pixels) / len(pixels)
    return mean


def extract_figure_from_pdf(file_bytes: bytes, figure: dict, dpi: int = 200) -> bytes | None:
    """Extract a figure region from a PDF page.

    **Strategy** (handles scanned PDFs with corrupted image layers):

    1.  Extract all embedded images from the figure's page.
    2.  For each embedded image, crop to the bounding-box region and score
        the result by visual quality (brightness — good scans are bright).
    3.  Return the best-scoring crop if it passes a minimum quality
        threshold.
    4.  Fall back to full-page rendering + PIL crop only when no embedded
        image produces a usable result.

    The bounding-box polygon values from Content Understanding are in
    *inches*.
    """
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

        # --- Attempt 1: extract from embedded images ---
        best_crop = None
        best_score = -1
        img_list = page.get_images(full=True)
        logging.info(
            f"[figure_extraction] Page {page_number}: {page_w}x{page_h}, "
            f"{len(img_list)} embedded images, polygon={polygon[:4]}..."
        )

        for img_info in img_list:
            xref = img_info[0]
            try:
                base = doc.extract_image(xref)
                if not base or not base.get("image"):
                    continue
                pil_img = Image.open(io.BytesIO(base["image"]))
                if pil_img.mode not in ("RGB", "L", "RGBA"):
                    pil_img = pil_img.convert("RGB")

                cropped = _crop_pil_image_to_bbox(
                    pil_img, polygon, page_w, page_h
                )
                if cropped is None or cropped.width < 10 or cropped.height < 10:
                    continue

                score = _image_quality_score(cropped)
                logging.info(
                    f"[figure_extraction] xref={xref} "
                    f"({pil_img.width}x{pil_img.height}) "
                    f"crop={cropped.width}x{cropped.height} "
                    f"quality={score:.1f}"
                )
                if score > best_score:
                    best_score = score
                    best_crop = cropped
            except Exception as exc:
                logging.info(
                    f"[figure_extraction] Could not process xref {xref}: {exc}"
                )

        # Accept the embedded-image crop if quality is reasonable
        # (mean brightness > 120 indicates a non-corrupted document image)
        if best_crop is not None and best_score > 120:
            logging.info(
                f"[figure_extraction] Using embedded image crop "
                f"(quality={best_score:.1f})"
            )
            buf = io.BytesIO()
            best_crop.save(buf, format="PNG")
            doc.close()
            return buf.getvalue()

        # --- Attempt 2: render full page then crop via PIL ---
        logging.info(
            "[figure_extraction] Falling back to page rendering "
            f"(best embedded score={best_score:.1f})"
        )
        zoom = dpi / 72
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        doc.close()

        xs = [polygon[i] * dpi for i in range(0, len(polygon), 2)]
        ys = [polygon[i] * dpi for i in range(1, len(polygon), 2)]
        left = max(0, int(min(xs)))
        top = max(0, int(min(ys)))
        right = min(img.width, int(max(xs)))
        bottom = min(img.height, int(max(ys)))

        cropped = img.crop((left, top, right, bottom))
        buf = io.BytesIO()
        cropped.save(buf, format="PNG")
        return buf.getvalue()
    except Exception as e:
        logging.error(f"[figure_extraction] PDF figure crop failed: {e}")
        return None


def _extract_images_from_ooxml(file_bytes: bytes, media_prefix: str) -> list[bytes]:
    """Extract all embedded images from an OOXML ZIP package.

    Images are returned sorted by filename (image1, image2, …) which
    typically matches document order.
    """
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


def _extract_all_pdf_images(file_bytes: bytes) -> list[bytes]:
    """Extract all embedded images from a PDF in page order.

    Used as a fallback when bounding-box information is not available
    (e.g. when Content Understanding does not return ``boundingRegions``).
    """
    import fitz  # PyMuPDF

    images: list[bytes] = []
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        for page in doc:
            for img_info in page.get_images(full=True):
                xref = img_info[0]
                base_image = doc.extract_image(xref)
                if base_image and base_image.get("image"):
                    images.append(base_image["image"])
        doc.close()
    except Exception as e:
        logging.error(f"[figure_extraction] PDF image extraction failed: {e}")
    return images


def build_figure_image_map(
    file_bytes: bytes,
    extension: str,
    figures: list[dict],
) -> dict[str, bytes]:
    """Build a mapping of *figure_id* → *image_bytes* for every figure.

    * **PDF** – each figure is cropped individually via its bounding box.
    * **DOCX** – all embedded images are extracted from ``word/media/`` and
      mapped to figures by document order.
    * **PPTX** – all embedded images are extracted from ``ppt/media/`` and
      mapped to figures by document order.

    Returns an empty dict when the format is unsupported or no images could
    be extracted.
    """
    image_map: dict[str, bytes] = {}

    if extension == "pdf":
        # Use bounding-box cropping when available; otherwise fall back to
        # extracting all embedded images and mapping them by order.
        has_bounds = any(fig.get("boundingRegions") for fig in figures)
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
            for idx, fig in enumerate(figures):
                fig_id = fig.get("id")
                if fig_id and idx < len(all_images):
                    image_map[fig_id] = all_images[idx]

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
