import io
import logging
import os
import base64
import re
from ..exceptions import UnsupportedFormatError
from .doc_analysis_chunker import DocAnalysisChunker
from tools import  BlobClient
from typing import List, Dict

class MultimodalChunker(DocAnalysisChunker):
    """
    MultimodalChunker processes documents containing both text and figures.
    It splits the document into chunks, replaces figure tags with identifiers,
    attaches figures to the appropriate chunks, and handles storage and captioning.
    """

    def __init__(self, data, max_chunk_size=None, minimum_chunk_size=None, token_overlap=None):
        """
        Initializes the MultimodalChunker with the given data and configuration.

        Args:
            data (dict): The document data to be processed.
            max_chunk_size (int, optional): Maximum number of tokens per chunk. Defaults to None.
            minimum_chunk_size (int, optional): Minimum number of tokens per chunk. Defaults to None.
            token_overlap (int, optional): Number of overlapping tokens between chunks. Defaults to None.
        """
        super().__init__(data, max_chunk_size, minimum_chunk_size, token_overlap)
        self.image_container = os.getenv("STORAGE_CONTAINER_IMAGES", "documents-images")
        self.storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME", "set-storage-account-name-env-var")
        self.minimum_figure_area_percentage = float(os.getenv("MINIMUM_FIGURE_AREA_PERCENTAGE", "4.0"))

    def get_chunks(self):
        """
        Retrieves and processes the document into chunks.

        Raises:
            UnsupportedFormatError: If the document's extension is not supported.
            Exception: If there are errors during document analysis.

        Returns:
            list: A list of processed document chunks.
        """
        if self.extension not in self.supported_formats:
            raise UnsupportedFormatError(f"[multimodal_chunker] {self.extension} format is not supported")

        logging.info(f"[multimodal_chunker][{self.filename}] Running get_chunks.")

        document, analysis_errors = self._analyze_document_with_retry()
        if analysis_errors:
            formatted_errors = ', '.join(map(str, analysis_errors))
            raise Exception(f"Error in doc_analysis_chunker analyzing {self.filename}: {formatted_errors}")

        chunks = self._process_document_chunks(document)
        return chunks

    def _process_document_chunks(self, document):
        """
        Processes the document by replacing figure tags, creating text chunks,
        and attaching figures to the corresponding chunks.

        Args:
            document (dict): The analyzed document containing content and figures.

        Returns:
            list: A list of processed document chunks.
        """
        # 1) Replace <figure>...</figure> with <figure{id}> in sequence
        if "figures" in document and document["figures"]:
            document["content"] = self._replace_figures_in_sequence(
                document["content"],
                document["figures"]
            )

        # 2) Create text chunks
        chunks = self._create_text_chunks(document)

        # 3) Attach figures to chunks
        self._attach_figures_to_chunks(document, chunks)

        return chunks  # Ensure chunks are returned



    def _replace_figures_in_sequence(self, content, figures):
        """
        Replace all occurrences of <figure>...</figure> with <figure{id}> in the order
        of the figures list. If we run out of <figure> tags or figures, we stop.

        Args:
            content (str): The document content containing figure tags.
            figures (list): A list of figure dictionaries with 'id' keys.

        Returns:
            str: The updated content with figure tags replaced by identifiers.
        """
        for fig in figures:
            figure_id = fig.get("id")
            if not figure_id:
                continue

            start_index = content.find("<figure>")
            if start_index == -1:
                break  # no more <figure> tags

            end_index = content.find("</figure>", start_index)
            if end_index == -1:
                break  # malformed or missing closing </figure>

            # Replace everything from <figure> to </figure> with <figure{id}>
            content = (
                content[:start_index] 
                + f"<figure{figure_id}>" 
                + content[end_index + len("</figure>"):]
            )

        return content

    def _create_text_chunks(self, document):
        """
        Splits the document content into chunks based on specified format and criteria.

        Args:
            document (dict): The document containing content to be chunked.

        Returns:
            list: A list of chunk dictionaries with content and metadata.
        """
        chunks = []
        document_content = document['content']
        document_content = self._number_pagebreaks(document_content)
        text_chunks = self._chunk_content(document_content)
        
        chunk_id = 0
        skipped_chunks = 0
        current_page = 1

        for text_chunk, num_tokens, chunk_offset, chunk_length in text_chunks:
            current_page = self._update_page(text_chunk, current_page)
            chunk_page = self._determine_chunk_page(text_chunk, current_page)

            if num_tokens >= self.minimum_chunk_size:
                chunk_id += 1
                chunk = self._create_chunk(
                    chunk_id=chunk_id,
                    content=text_chunk,
                    page=chunk_page,
                    offset=chunk_offset,
                )
                chunks.append(chunk)
            else:
                skipped_chunks += 1
        
        logging.debug(f"[multimodal_chunker][{self.filename}] {len(chunks)} chunk(s) created")
        if skipped_chunks > 0:
            logging.debug(f"[multimodal_chunker][{self.filename}] {skipped_chunks} chunk(s) skipped")

        return chunks

    def _chunk_content(self, content):
        """
        Splits the document content into chunks based on the specified format and criteria.
        
        Yields:
            tuple: A tuple containing:
                   (chunked_content, number_of_tokens, chunk_offset, chunk_length)
        """
        splitter = self._choose_splitter()

        chunks = splitter.split_text(content)

        offset = 0
        for chunked_content in chunks:
            chunk_size = self.token_estimator.estimate_tokens(chunked_content)
            chunk_length = len(chunked_content)
            yield chunked_content, chunk_size, offset, chunk_length
            offset += chunk_length

    def _attach_figures_to_chunks(self, document, chunks):
        """
        Associates figures from the document with their corresponding text chunks.
        by scanning each chunk for <figureX.Y> placeholders.

        For each figure reference in a chunk:
        1) Retrieve the figure from document["figures"] by ID
        2) Upload the image to Blob Storage
        3) Generate descriptions (captions)
        4) Generate embeddings
        5) Build one combined caption string that references all figures in this chunk
        6) Attach caption and embeddings to the chunk via metodo_append_figures_to_chunk
        """

        if "figures" not in document or not document["figures"]:
            logging.info(f"[multimodal_chunker][{self.filename}] No figures to attach.")
            return

        result_id = document.get("result_id")
        model_id = document.get("model_id")
        if not result_id or not model_id:
            logging.warning(
                f"[multimodal_chunker][{self.filename}] Missing 'result_id' or 'model_id' in document analysis results."
            )
            return

        logging.info(
            f"[multimodal_chunker][{self.filename}] Attaching figures to chunks using "
            f"result_id: {result_id} and model_id: {model_id}."
        )

        # Create a quick-access dictionary for the figures by their ID
        figures_dict = {fig["id"]: fig for fig in document["figures"] if "id" in fig}

        # Regex to find all <figureX.Y> (or <figureX> if single integer)
        figure_tag_pattern = re.compile(r"<figure(\d+(?:\.\d+)*)>")

        for chunk in chunks:
            chunk_content = chunk.get("content", "")
            figure_refs = figure_tag_pattern.findall(chunk_content)
            if not figure_refs:
                # No figure references in this chunk; move to the next
                continue

            # Build arrays to store references for this chunk
            figure_urls = []
            figure_descriptions = []

            for figure_id in figure_refs:
                # Attempt to find the figure in the dictionary
                figure = figures_dict.get(figure_id)
                if not figure:
                    logging.warning(
                        f"[multimodal_chunker][{self.filename}] Figure with id={figure_id} not found in document['figures']."
                    )
                    chunk_content = chunk_content.replace(f"<figure{figure_id}>", "")
                    continue

                try:
                    # 1) Check dimensions
                    figure_area_percentage = round(self._figure_area(figure, document['pages']), 2)
                    if figure_area_percentage <= self.minimum_figure_area_percentage:
                        logging.warning(
                            f"[multimodal_chunker][{self.filename}] Image for figure {figure_id} "
                            f"has insufficient percentual area ({figure_area_percentage}). Skipping."
                        )
                        chunk_content = chunk_content.replace(f"<figure{figure_id}>", "")
                        continue

                    # 2) Fetch the figure image
                    image_binary = self.docint_client.get_figure(model_id, result_id, figure_id)
                    if not image_binary:
                        logging.warning(
                            f"[multimodal_chunker][{self.filename}] No image data retrieved for figure {figure_id}."
                        )
                        chunk_content = chunk_content.replace(f"<figure{figure_id}>", "")
                        continue

                    # Check dimensions
                    # image = Image.open(io.BytesIO(image_binary))
                    # width, height = image.size
                    # pixel_area = width * height
                    # if pixel_area <= self.minimum_pixel_area:
                    #     logging.warning(
                    #         f"[multimodal_chunker][{self.filename}] Image for figure {figure_id} "
                    #         f"has insufficient pixel area ({pixel_area}). Skipping."
                    #     )
                    #     chunk_content = chunk_content.replace(f"<figure{figure_id}>", "")
                    #     continue


                    # 3) Upload to blob
                    blob_name = f"{self.filename}-figure-{figure_id}.png"
                    url = self._upload_figure_blob(image_binary, blob_name)

                    # 4) Generate caption
                    logging.info(f"[multimodal_chunker][{self.filename}] Generating caption for figure {figure_id}. Percent area: {figure_area_percentage}")                    
                    figure_caption = self._generate_caption_for_figure(
                        {
                            "id": figure_id,
                            "image": base64.b64encode(image_binary).decode("utf-8"),
                            "blob_name": blob_name
                        }
                    )

                    # Store references
                    figure_urls.append(url)
                    figure_descriptions.append(f"[{blob_name}]: {figure_caption}")

                    # Replace <figureX.Y> with a simpler marker or remove it
                    chunk_content = chunk_content.replace(f"<figure{figure_id}>", f"<figure>{blob_name}</figure>")

                except Exception as e:
                    logging.error(
                        f"[multimodal_chunker][{self.filename}] Error processing figure {figure_id}: {str(e)}"
                    )


            # Update the chunk content with placeholders updated 
            chunk["content"] = chunk_content

            # 5) Build the combined caption string
            #    Example:
            #    [myfile-figure-1.1.png]: figure (myfile-figure-1.1.png) description: ...
            #    [myfile-figure-1.2.png]: figure (myfile-figure-1.2.png) description: ...
            combined_caption = "\n".join(figure_descriptions)

            caption_vector = self.aoai_client.get_embeddings(combined_caption)

            # 6) Attach everything to the chunk
            if figure_urls or combined_caption:
                self._append_figures_to_chunk(
                    chunk,
                    figure_urls,
                    combined_caption,
                    caption_vector
                )
                logging.info(f"[multimodal_chunker][{self.filename}] Attached {len(figure_urls)} figures to chunk {chunk['chunk_id']}.") 

    def _figure_area(self, figure: Dict, pages: List[Dict]) -> float:
        """
        Calculate the total figure area by summing the areas of all bounding regions across pages.
        
        Args:
            figure (Dict): A dictionary representing the figure with 'boundingRegions', 
                        where each bounding region contains 'pageNumber' and 'polygon'.
            pages (List[Dict]): A list of page dictionaries each containing 'pageNumber', 'width', and 'height'.
        
        Returns:
            float: The total area of all valid bounding regions across pages.
                Returns 0.0 if no valid bounding regions are found or an error occurs.
        """
        total_area = 0.0

        # Ensure 'boundingRegions' exists in the figure
        bounding_regions = figure.get('boundingRegions', [])
        if not bounding_regions:
            logging.warning(f"[multimodal_chunker][{self.filename}] No boundingRegions found in figure.")
            return total_area  # Returns 0.0

        # Create a lookup dictionary for pages to optimize performance
        page_lookup = {page['pageNumber']: page for page in pages}

        for idx, bounding_region in enumerate(bounding_regions, start=1):
            try:
                # Extract bounding region details
                page_number = bounding_region['pageNumber']
                polygon = bounding_region['polygon']
            except KeyError as e:
                logging.error(f"[multimodal_chunker][{self.filename}] Bounding region {idx} is missing key: {e}")
                continue  # Skip this bounding region

            # Find the corresponding page using the lookup dictionary
            page = page_lookup.get(page_number)
            if not page:
                logging.info(f"[multimodal_chunker][{self.filename}] No matching page found for pageNumber: {page_number} in bounding region {idx}.")
                continue  # Skip this bounding region

            page_width = page.get('width')
            page_height = page.get('height')

            # Validate page dimensions
            if page_width is None or page_height is None:
                logging.error(f"[multimodal_chunker][{self.filename}] Page {page_number} is missing 'width' or 'height'.")
                continue  # Skip this bounding region

            try:
                # Calculate polygon area using a helper method
                polygon_area = self._calculate_polygon_area(polygon)
            except ValueError as ve:
                logging.error(f"[multimodal_chunker][{self.filename}] Error calculating area for figure on page {page_number}, bounding region {idx}: {ve}")
                continue  # Skip this bounding region

            # Optionally, validate that the polygon area does not exceed the page area
            page_area = page_width * page_height
            if polygon_area > page_area:
                logging.warning(
                    f"[multimodal_chunker][{self.filename}] Polygon area {polygon_area:.2f} exceeds page area {page_area:.2f} on page {page_number}, bounding region {idx}."
                )
                # Depending on requirements, we might choose to:
                # - Skip adding this area
                # - Cap the polygon area to the page area
                # - Include the area as is (current implementation)
                # Here, we'll include it.

            # Accumulate the total area
            total_area += polygon_area

            logging.debug(
                f"[multimodal_chunker][{self.filename}] Figure on Page {page_number}, Bounding Region {idx}: "
                f"Polygon Coordinates: {polygon}, "
                f"Polygon Area: {polygon_area:.2f}, "
                f"Accumulated Total Area: {total_area:.2f}"
            )

        if total_area == 0.0:
            logging.warning(f"[multimodal_chunker][{self.filename}] No valid bounding regions found to calculate total area.")

        return total_area

    def _calculate_polygon_area(self, polygon: List[float]) -> float:
        """
        Calculate the area of a polygon using the Shoelace formula.

        Args:
            polygon (List[float]): A list of coordinates [x1, y1, x2, y2, ..., xn, yn].

        Returns:
            float: The absolute area of the polygon.
        """
        if len(polygon) < 6:
            raise ValueError("A polygon must have at least 3 points (6 coordinates).")

        area = 0.0
        num_points = len(polygon) // 2
        for i in range(num_points):
            x1, y1 = polygon[2 * i], polygon[2 * i + 1]
            x2, y2 = polygon[2 * ((i + 1) % num_points)], polygon[2 * ((i + 1) % num_points) + 1]
            area += (x1 * y2) - (x2 * y1)
        return abs(area) / 2.0


    def _append_figures_to_chunk(self, chunk, figure_urls, combined_caption, figure_vector):
        """
        Appends the combined figure data (URLs, a single combined caption string,
        and a list of caption vectors) to the chunk.
        """
        # 1) Related images (URLs)
        if "relatedImages" not in chunk:
            chunk["relatedImages"] = []
        chunk["relatedImages"].extend(figure_urls)

        # 2) Combined caption text
        #    Storing in a new field named "caption" (or "imageCaptions" as needed)
        if "caption" not in chunk:
            chunk["caption"] = ""
        if chunk["caption"]:
            chunk["caption"] += "\n"
        chunk["caption"] += combined_caption

        # 3) Combine vectors
        #    You may store all figure vectors as a list or you could average them, etc.
        if "vectorCaption" not in chunk:
            chunk["vectorCaption"] = []
        if isinstance(chunk["vectorCaption"], list):
            chunk["vectorCaption"].extend(figure_vector)
        else:
            logging.warning(f"[metodo_append_figures_to_chunk] 'vectorCaption' is not a list in chunk {chunk.get('chunk_id')}")


    def _find_chunks_for_figure(self, figure_id, chunks):
        """
        Searches for all chunks whose content contains <figure{figure_id}>.
        Returns a list of such chunks.

        Args:
            figure_id (str): The identifier of the figure to find.
            chunks (list): The list of text chunks to search within.

        Returns:
            list: A list of chunks containing the figure placeholder.
        """
        matched_chunks = []

        for chunk in chunks:
            chunk_content = chunk.get("content", "")
            if f"<figure{figure_id}>" in chunk_content:
                matched_chunks.append(chunk)

        if not matched_chunks:
            # Log a warning if no chunks contain <figure{id}>
            logging.warning(f"[multimodal_chunker][{self.filename}] Could not find <figure{figure_id}> in any chunk.")

        return matched_chunks

    def _upload_figure_blob(self, image_bytes, blob_name):
        """
        Uploads the image bytes to Blob Storage and returns the SAS URL.

        Args:
            image_bytes (bytes): The binary data of the image to upload.
            blob_name (str): The name to assign to the uploaded blob.

        Returns:
            str: The URL of the uploaded blob, or an empty string if upload fails.
        """
        try:
            blob_url = f"https://{self.storage_account_name}.blob.core.windows.net/{self.image_container}/{blob_name}"
            blob_client = BlobClient(blob_url)
            blob_service_client = blob_client.blob_service_client
            container_client = blob_service_client.get_container_client(blob_client.container_name)
            blob_client_instance = container_client.get_blob_client(blob_client.blob_name)
            blob_client_instance.upload_blob(image_bytes, overwrite=True)
            return blob_url
        except Exception as e:
            logging.error(f"[multimodal_chunker][{self.filename}] Failed to upload figure {blob_name}: {str(e)}")
            return ""

    def _generate_caption_for_figure(self, figure):
        """
        Generates a caption for the figure using the Azure OpenAI client.

        Args:
            figure (dict): The figure data containing image information.

        Returns:
            str: The generated caption for the figure.
        """
        try:
            caption_prompt = (
                "Generate a detailed description of the following figure, including "
                "its key elements and context, to optimize it for retrieval purposes. "
                "Use no more than 200 words."
            )
            caption = self.aoai_client.get_completion(
                prompt=caption_prompt, 
                image_base64=figure["image"]
            )
            logging.debug(f"[multimodal_chunker][{self.filename}] Generated caption for figure {figure.get('id', 'unknown')}: {caption}")
            return caption
        except Exception as e:
            logging.error(f"[multimodal_chunker][{self.filename}] Failed to generate caption for figure {figure.get('id', 'unknown')}: {str(e)}")
            return "No caption available."

    def _append_figure_to_chunk(self, chunk, url, caption, caption_vector):
        """
        Appends the figure's URL, caption, and embedding vector to the specified chunk.

        Args:
            chunk (dict): The chunk to which the figure information will be appended.
            url (str): The URL of the uploaded figure image.
            caption (str): The generated caption for the figure.
            caption_vector (list): The embedding vector of the caption.
        """
        # Append the image URL
        if "relatedImages" not in chunk:
            chunk["relatedImages"] = []
        chunk["relatedImages"].append(url)
        logging.debug(f"[multimodal_chunker][{self.filename}] Added image URL to chunk {chunk.get('chunk_id')}")

        # Append the image caption
        if "imageCaptions" not in chunk:
            chunk["imageCaptions"] = ""
        chunk["imageCaptions"] += f"{caption}\n"
        logging.debug(f"[multimodal_chunker][{self.filename}] Added caption to chunk {chunk.get('chunk_id')}")

        # Append the caption vector
        if "imageCaptionsVector" not in chunk:
            chunk["imageCaptionsVector"] = []
        if isinstance(chunk["imageCaptionsVector"], list):
            chunk["imageCaptionsVector"].append(caption_vector)
            logging.debug(f"[multimodal_chunker][{self.filename}] Added caption vector to chunk {chunk.get('chunk_id')}")
        else:
            logging.warning(f"[multimodal_chunker][{self.filename}] imageCaptionsVector is not a list in chunk {chunk.get('chunk_id')}")
