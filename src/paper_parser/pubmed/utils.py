"""Utility functions for PubMed/PMC XML parsing."""

from lxml import etree
import os
import regex as re

from paper_parser.shared.schemas import DEFAULT_REF_TYPE

def describe_source(x: str | etree.ElementTree) -> str:
    """Best-effort human-readable description of the XML source."""
    if isinstance(x, str):
        return x
    return f"<{type(x).__name__}>"


def build_xml_tree(x: str | etree.ElementTree) -> etree.ElementTree:
    """Build an XML tree from a filepath or XML string.
    
    Args:
        x: Path to XML file or XML content as string
        
    Returns:
        Parsed XML tree
        
    Raises:
        ValueError: If input is not a string or is empty
    """
    if isinstance(x, etree.ElementTree):
        return x
    
    if not isinstance(x, str) or not x.strip():
        raise ValueError("Input must be a non-empty string containing a filepath or XML content")

    if os.path.isfile(x):
        x_string = open(x, 'r').read()
    else:
        x_string = x

    # Check whether xlink namespace is defined, adding if necessary
    parser = etree.XMLParser(recover=True)
    root = etree.fromstring(x_string.encode("utf-8"), parser)
    if "xlink" not in root.nsmap:
        root.set(
            "{http://www.w3.org/2000/xmlns/}xlink",
            "http://www.w3.org/1999/xlink"
        )
    return etree.ElementTree(root)


def get_xml_root(x: str | etree.ElementTree) -> etree.Element:
    """Get the root element of an XML tree."""
    return build_xml_tree(x).getroot()

def strip_whitespace(x: str) -> str:
    """Strip whitespace from a string."""
    return x.strip()


def stringify(node: etree.Element, recurse: bool = True, delimiter: str | None = None) -> str | list[str]:
    """Convert XML node to string or list of strings.
    
    Args:
        node: XML element to stringify
        recurse: Whether to recursively process child nodes
        delimiter: If provided, join parts with this delimiter (returns str).
                   If None, returns list of strings.
    
    Returns:
        String if delimiter is provided, otherwise list of strings
    """
    if node is None:
        return ""
    
    parts = []
    if node.text:
        parts.append(node.text)
    if recurse:
        for child in node:
            parts.extend(stringify(child))
            if child.tail:
                parts.append(child.tail)
    if isinstance(delimiter, str):
        return delimiter.join(parts)
    else:
        return parts


def deduplicate(x: list | tuple, keep_order: bool = False) -> list:
    """Remove duplicates from a list or tuple.
    
    Args:
        x: List or tuple to deduplicate
        keep_order: If True, preserve original order
        
    Returns:
        List with duplicates removed
    """
    if keep_order:
        temp = set()
        out = []
        for item in x:
            if item not in temp:
                out.append(item)
                temp.add(item)
        return out
    else:
        return list(set(x))


def get_xml_lang(node: etree.Element) -> str | None:
    """Get language attribute from XML node.
    
    Tries xml:lang first, then lang attribute.
    
    Args:
        node: XML element
        
    Returns:
        Language code or None if not found
    """
    return node.get("xml:lang") or node.get("lang")


MONTH_NAME_TO_NUMBER_MAP = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5, 
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}


def convert_month_name_to_number(month_name: str) -> int:
    """Convert month name to number.
    
    Args:
        month_name: Month name (case-insensitive)
        
    Returns:
        Month number (1-12)
        
    Raises:
        KeyError: If month name is not recognized
    """
    return MONTH_NAME_TO_NUMBER_MAP[month_name.lower()]


PMCOA_XREF_REF_TYPE_MAP: dict[str, str] = {
    "bibr": "bib_entry",
    "fig": "figure",
    "table": "figure",
    "sec": "section",
}


def normalize_pmcoa_ref_type(
    ref_type: str | None, default_ref_type: str = DEFAULT_REF_TYPE
) -> str | None:
    """Normalize a PMC OA xref ref-type attribute to a schema-compatible value."""
    rt = (ref_type or "").strip().lower()
    if not rt:
        return None
    return PMCOA_XREF_REF_TYPE_MAP.get(rt, default_ref_type)


def _local_tag(el: etree._Element) -> str:
    """Return local tag name without namespace. Handles lxml QName (callable .tag)."""
    tag = el.tag
    if callable(tag):
        try:
            tag = tag()
        except TypeError:
            tag = getattr(tag, "__name__", None)
    tag = str(tag)
    return tag.split("}")[-1] if "}" in tag else tag


def _find_child(parent: etree._Element, local_name: str) -> etree._Element | None:
    """Find direct child element by local tag name (ignores namespace)."""
    for child in parent:
        if _local_tag(child) == local_name:
            return child
    return None

