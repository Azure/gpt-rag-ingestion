"""
SharePointMetadataStreamer

This module defines a class that connects to Microsoft Graph using your tenant ID, client ID, and client secret.
It locates the SharePoint site you specify and then walks through each folder and its subfolders. 
As it goes, it gathers information about each file—such as its name, full path, size, and last modified timestamp—and yields those details one file at a time without downloading file contents.

You can provide a list of file extensions (for example, "pdf" or "docx") to restrict the output to only those types. 
Every 10 files found, it logs a simple progress message so you always know how many files have been processed so far.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Union
import msal
import os
import requests
import re
import logging

from dependencies   import get_config

app_config_client = get_config()

GREEN = "\033[32m"
ORANGE = "\033[38;5;208m"
RESET = "\033[0m"

class SharePointMetadataStreamer:
    """Facilitates streaming of file metadata from SharePoint via Microsoft Graph API. Supports filtering by folder names and file formats."""

    def __init__(
        self,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        graph_uri: str = "https://graph.microsoft.com",
        authority_template: str = "https://login.microsoftonline.com/{tenant_id}",
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.graph_uri = graph_uri
        self.authority = (
            authority_template.format(tenant_id=tenant_id) if tenant_id else None
        )
        self.scope = ["https://graph.microsoft.com/.default"]
        self.access_token: Optional[str] = None
        self._file_count = 0
        self._max_file_count = int(app_config_client.get("SHAREPOINT_MAX_FILE_COUNT", -1))

    def stream_file_metadata(
        self,
        site_domain: str,
        site_name: str,
        drive_id: str,
        folders_names: List[str],
        folder_regex: Optional[str] = None,
        file_formats: Optional[List[str]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Stream file metadata under specified folders (and their subfolders).
        Does not download file content; yields one item at a time.
        """
        if self._are_required_variables_missing():
            return

        site_id = self._get_site_id(site_domain, site_name)
        if not site_id:
            return

        self._msgraph_auth()
        self._file_count = 0

        # Determine root paths to traverse
        if not folders_names or folders_names == ['/']:
            paths_to_traverse = ['']
        else:
            paths_to_traverse = [name.strip('/"') for name in folders_names]

        for path in paths_to_traverse:
            clean_path = path.replace('"', '')
            try:
                yield from self._stream_files(site_id, drive_id, clean_path, folder_regex, file_formats)
            except Exception as e:
                logging.error(f"[sharepoint] Error traversing '{clean_path}': {e}")

    def _stream_files(
        self,
        site_id: str,
        drive_id: str,
        rel_path: str,
        folder_regex: str,        
        file_formats: Optional[List[str]] = None,
    ) -> Iterator[Dict[str, Any]]:
        # Helper to recursively traverse and yield metadata

        # Stop recursion immediately if max file count is reached
        if self._max_file_count > 0 and self._file_count >= self._max_file_count:
            return

        if rel_path:
            next_url = (
                f"{self.graph_uri}/v1.0/sites/{site_id}"
                f"/drives/{drive_id}/root:/{rel_path}:/children"
            )
        else:
            next_url = (
                f"{self.graph_uri}/v1.0/sites/{site_id}"
                f"/drives/{drive_id}/root/children"
            )

        while next_url:
            # Stop if max file count is reached before making the request
            if self._max_file_count > 0 and self._file_count >= self._max_file_count:
                logging.info(
                    f"[sharepoint] Reached max file count limit: {self._max_file_count}. Stopping."
                )
                return

            logging.info(f"[sharepoint] Fetching page: {next_url}")
            resp = self._make_ms_graph_request(next_url)

            for item in resp.get("value", []):
                # Stop if max file count is reached before yielding more files or recursing
                if self._max_file_count > 0 and self._file_count >= self._max_file_count:
                    logging.info(
                        f"[sharepoint] Reached max file count limit: {self._max_file_count}. Stopping."
                    )
                    return
                if "folder" in item:
                    folder_name = item['name']
                    # Ignore folders that do not match the folder regex
                    if folder_regex and rel_path == ''  and not re.match(folder_regex, folder_name):
                        continue
                    child = f"{rel_path}/{item['name']}" if rel_path else item['name']
                    yield from self._stream_files(site_id, drive_id, child, folder_regex, file_formats)

                elif "file" in item:
                    # ignore files that are in root folder when folder_regex is specified
                    if rel_path == '' and folder_regex != '.*':
                        continue
                    # ignore files that do not match the file extension filter
                    if file_formats and not any(
                        item['name'].lower().endswith(f".{ext.strip().lower()}")
                        for ext in file_formats
                    ):
                        continue

                    # Increment and log progress
                    self._file_count += 1
                    if self._file_count % 10 == 0:
                        logging.info(
                            ORANGE + f"[sharepoint] Selected {self._file_count} files so far" + RESET
                        )

                    logging.info(
                        GREEN + f"[sharepoint] Selected file: {item['parentReference']['path']}/{item['name']}" + RESET
                    )

                    yield item

            next_url = resp.get('@odata.nextLink')

    def _msgraph_auth(self):
        if not all([self.client_id, self.client_secret, self.authority]):
            raise ValueError("Missing required authentication credentials.")
        app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            authority=self.authority,
            client_credential=self.client_secret,
        )
        token = app.acquire_token_silent(self.scope, account=None)
        if not token:
            token = app.acquire_token_for_client(scopes=self.scope)
        if not token or "access_token" not in token:
            raise RuntimeError("Failed to acquire access token")
        self.access_token = token["access_token"]

    def _get_site_id(
        self,
        site_domain: str,
        site_name: str,
    ) -> Optional[str]:
        url = f"{self.graph_uri}/v1.0/sites/{site_domain}:/sites/{site_name}:/"
        resp = self._make_ms_graph_request(url)
        return resp.get("id")

    def _make_ms_graph_request(
        self,
        url: str,
    ) -> Dict:
        if not self.access_token:
            raise ValueError("Access token is required for graph requests")
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def _are_required_variables_missing(self) -> bool:
        missing = [v for v in [self.tenant_id, self.client_id, self.client_secret, self.authority] if not v]
        if missing:
            logging.error("[sharepoint] Missing required credentials.")
            return True
        return False

    def _get_files(
        self,
        site_id: str,
        drive_id: str,
        folders_names: List[str],
        file_formats: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        List all files under each folder in folders_names, recursively,
        handling pagination of Microsoft Graph results.
        Use ['/',] or [] to start from root.
        """
        # Determine root paths to traverse
        if not folders_names or folders_names == ['/']:
            paths_to_traverse = ['']
        else:
            paths_to_traverse = [name.strip('/"') for name in folders_names]

        collected: List[Dict[str, Any]] = []
        file_count = 0

        def traverse(rel_path: str):
            # Start with the first page URL
            if rel_path:
                next_url = (
                    f"{self.graph_uri}/v1.0/sites/{site_id}"
                    f"/drives/{drive_id}/root:/{rel_path}:/children"
                )
            else:
                next_url = (
                    f"{self.graph_uri}/v1.0/sites/{site_id}"
                    f"/drives/{drive_id}/root/children"
                )

            # Loop through all pages
            while next_url:
                logging.info(f"[sharepoint] Fetching page: {next_url}")
                resp = self._make_ms_graph_request(next_url, self.access_token)

                for item in resp.get("value", []):
                    if "folder" in item:
                        # Recurse into subfolder
                        child = f"{rel_path}/{item['name']}" if rel_path else item['name']
                        # if child.lower().startswith("practica") and "_2025" in child:
                        logging.info(f"CHILD : {child}")
                        traverse(child)

                    elif "file" in item:
                        # Apply format filter if provided
                        if file_formats and not any(
                            item['name'].lower().endswith(f".{ext.strip().lower()}")
                            for ext in file_formats
                        ):
                            continue

                        # Count and log progress every 50 files
                        nonlocal_file_count = globals().get('file_count', None)
                        # Actually increment our file_count in the outer scope
                        # Python scoping workaround:
                        # we refer to file_count via a mutable container or nonlocal in Python 3
                        # Simplest: declare file_count as nonlocal
                        # (move file_count definition into an enclosing scope if needed)

                        # For clarity here, assume file_count is nonlocal:
                        nonlocal file_count
                        file_count += 1
                        if file_count % 10 == 0:
                            logging.info(ORANGE + f"[sharepoint] Selected {file_count} files so far" + RESET)

                        logging.info(GREEN + f"[sharepoint] Selected file: " + f"{item['parentReference']['path']}/{item['name']}" + RESET)
                        collected.append(item)

                # Check for next page
                next_url = resp.get('@odata.nextLink')

        for path in paths_to_traverse:
            clean_path = path.replace('"', '')
            try:
                # if clean_path.lower().startswith("practica") and "_2025" in clean_path:
                logging.info(f"CLEAN PATH {clean_path}")
                traverse(clean_path)
            except Exception as e:
                logging.error(f"[sharepoint] Error traversing '{clean_path}': {e}")
                continue

        return collected

    def _process_files(
        self,
        site_id: str,
        drive_id: str,
        files: List[Dict[str, Any]],
        file_formats: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Download content and extract metadata for each file.
        """
        results: List[Dict[str, Any]] = []
        for item in files:
            name = item.get('name')
            if not name:
                continue
            metadata = self._extract_file_metadata(item)
            content = self._get_file_content_bytes(site_id, drive_id, item)
            permissions = self._get_file_permissions(site_id, drive_id, item['id'])
            readers = self._get_read_access_entities(permissions)
            entry = {
                'content': content,
                **metadata,
                'read_access_entities': readers,
            }
            results.append(entry)
        return results

    def _get_file_content_bytes(
        self,
        site_id: str,
        drive_id: str,
        item: Dict[str, Any],
    ) -> bytes:
        if not self.access_token:
            raise ValueError("Missing access token for download")
        path = item['parentReference']['path']
        rel = path.split('root:')[-1]
        url = (
            f"{self.graph_uri}/v1.0/sites/{site_id}/drives/{drive_id}/root:"
            f"{rel}/{item['name']}:/content"
        )
        response = requests.get(url, headers={"Authorization": f"Bearer {self.access_token}"})
        response.raise_for_status()
        return response.content

    def _extract_file_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        def z(dt: str) -> str:
            return dt if dt.endswith('Z') else dt + 'Z'
        info = data.get('fileSystemInfo', {})
        return {
            'id': data.get('id'),
            'source': data.get('webUrl'),
            'name': data.get('name'),
            'size': data.get('size'),
            'created_by': data.get('createdBy', {}).get('user', {}).get('displayName'),
            'created_datetime': z(info.get('createdDateTime', '')) if info.get('createdDateTime') else None,
            'last_modified_datetime': z(info.get('lastModifiedDateTime', '')) if info.get('lastModifiedDateTime') else None,
            'last_modified_by': data.get('lastModifiedBy', {}).get('user', {}).get('displayName'),
        }

    def _get_file_permissions(
        self,
        site_id: str,
        drive_id: str,
        item_id: str,
    ) -> List[Dict[str, Any]]:
        url = f"{self.graph_uri}/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}/permissions"
        resp = self._make_ms_graph_request(url)
        return resp.get('value', [])

    def _get_read_access_entities(self, permissions: List[Dict[str, Any]]) -> List[str]:
        readers: List[str] = []
        for perm in permissions:
            if not isinstance(perm, dict) or 'roles' not in perm:
                continue
            if any(r in perm.get('roles', []) for r in ['read', 'write']):
                for ident in perm.get('grantedToIdentitiesV2', []):
                    uid = ident.get('user', {}).get('id')
                    if uid and uid not in readers:
                        readers.append(uid)
        return readers

    def _are_required_variables_missing(self) -> bool:
        missing = [v for v in [self.tenant_id, self.client_id, self.client_secret, self.authority] if not v]
        if missing:
            logging.error("[sharepoint] Missing required credentials.")
            return True
        return False
