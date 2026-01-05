"""PMC XML parser for extracting article metadata and content."""

import logging
from lxml import etree
from .utils import (
    build_xml_tree,
    get_xml_root,
    stringify,
    get_xml_lang,
    convert_month_name_to_number,
    describe_source,
)
from .schemas import (
    ArticleIDs,
    Date,
    Reference,
    Article,
)

logger = logging.getLogger(__name__)


def extract_article_type(x: str | etree.ElementTree) -> str | None:
    """Extract article type from XML.
    
    Compared to titipata/pubmed_parser:
    - This feature is new
    
    For more information, see https://jats.nlm.nih.gov/archiving/tag-library/1.4/attribute/article-type.html
    
    Args:
        x: Path to XML file or XML tree
        
    Returns:
        Article type string or None
    """
    root = get_xml_root(x)
    return root.get("article-type", None)


def extract_article_ids(x: str | etree.ElementTree) -> ArticleIDs:
    """Extract article IDs.
    
    Compared to titipata/pubmed_parser:
    - Extracts all ID information available
    - Avoids hardcoding
    - Validates output using ArticleIDs schema

    For more information, see 
    - https://jats.nlm.nih.gov/archiving/tag-library/1.4/element/article-id.html
    - https://jats.nlm.nih.gov/archiving/tag-library/1.4/attribute/pub-id-type.html
    
    Args:
        x: Path to XML file or XML tree
        
    Returns:
        ArticleIDs model instance with validated publication IDs
        
    Raises:
        ValueError: If extracted data doesn't meet schema validation requirements
    """
    root = get_xml_root(x)
    # Extract all article-id elements from the article metadata section
    out = {}
    for node in root.xpath("front/article-meta/article-id"):
        pub_id_type = node.attrib.get("pub-id-type")
        article_id = (node.text or "").strip() or None

        # Only add valid pub-id-type and article_id pairs
        if pub_id_type and article_id:
            out[pub_id_type] = article_id
        else:
            # Warn about unusual cases (missing type or ID)
            logger.warning(
                "Unusual article-id element (missing type or id); pub_id_type=%r article_id=%r source=%s",
                pub_id_type,
                article_id,
                describe_source(x),
            )
    
    # Validate and return as ArticlePubIds schema
    try:
        return ArticleIDs(**out)
    except Exception as e:
        raise ValueError(f"Invalid article IDs from {describe_source(x)}: {out}") from e


def extract_subjects(x: str | etree.ElementTree) -> list[str]:
    """Extract article subjects/categories.
    
    Compared to titipata/pubmed_parser:
    - Returns list of subjects instead of concatenation
    - Use absolute path to avoid duplicate information from front-stubs 

    TODO:
    - Figure out how to filter out non-subjecty information like "Case Report"
        - subj-group-type does not look useful

    For more information, see
    - https://jats.nlm.nih.gov/archiving/tag-library/1.4/element/article-categories.html
    - https://jats.nlm.nih.gov/archiving/tag-library/1.4/element/subj-group.html
    - https://jats.nlm.nih.gov/archiving/tag-library/1.4/element/subject.html
    
    Args:
        x: Path to XML file or XML tree
        
    Returns:
        list of subject strings
    """
    root = get_xml_root(x)

    # Use // to find all subject nodes nested within subj-group elements
    # This captures subjects from all subj-group hierarchies
    out = []
    for node in root.xpath("front/article-meta/article-categories//subj-group/subject"):
        # Join text content with spaces to create readable subject strings
        subject = stringify(node, delimiter=" ")
        if subject:
            out.append(subject)
    return out


def extract_title(x: str | etree.ElementTree) -> str | None:
    """Extract article title.
    
    Compared to titipata/pubmed_parser:
    - Separate from subtitle
    - Take only first title group, because other title groups are usually for different languages

    TODO:
    - Consider how to handle multiple title groups (normally for different languages)

    For more information, see
    - https://jats.nlm.nih.gov/archiving/tag-library/1.4/element/title-group.html
    - https://jats.nlm.nih.gov/archiving/tag-library/1.4/element/article-title.html
    
    Args:
        x: Path to XML file or XML tree
        
    Returns:
        Article title string or None
    """
    root = get_xml_root(x)

    # Find all article-title nodes within title-group elements
    # Use // to search recursively through nested structures
    nodes = root.xpath("front/article-meta//title-group/article-title")
    if nodes:
        return stringify(nodes[0], delimiter=" ")
    return None


def extract_subtitle(x: str | etree.ElementTree) -> str | None:
    """Extract article subtitle.
    
    Compared to titipata/pubmed_parser:
    - Not combined with title

    For more information, see
    - https://jats.nlm.nih.gov/archiving/tag-library/1.4/element/title-group.html
    - https://jats.nlm.nih.gov/archiving/tag-library/1.4/element/subtitle.html
    
    Args:
        x: Path to XML file or XML tree
        
    Returns:
        Article subtitle string or None
    """
    root = get_xml_root(x)

    # Find subtitle nodes within title-group elements
    nodes = root.xpath("front/article-meta//title-group/subtitle")
    if nodes:
        return stringify(nodes[0], delimiter=" ")
    return None


def extract_abstract(x: str | etree.ElementTree) -> str | None:
    """Extract article abstract.
    
    Compared to titipata/pubmed_parser:
    - Look only within article-meta element, because even figures can have abstracts
    - When multiple <abstract> nodes exist, prefer:
        1) those with xml:lang="en" (or starting with "en"),
        2) the longest stringified text,
        3) those without an "abstract-type" attribute, because the full abstracts usually have no type.
    - Not every node has these attributes, so we treat them as soft preferences.

    For more information, see
    - https://jats.nlm.nih.gov/archiving/tag-library/1.4/element/abstract.html
    
    Args:
        x: Path to XML file or XML tree
        
    Returns:
        Abstract text string or None
    """
    root = get_xml_root(x)

    # Find all abstract elements within article-meta (not in figures, etc.)
    nodes = root.xpath("front/article-meta/abstract")
    if not nodes:
        return None

    def score_node(node: etree.Element) -> tuple:
        """Score abstract node for preference.
        
        Returns tuple of (is_english, text_length, has_no_abstract_type).
        Higher scores are preferred. The tuple comparison works because:
        - English abstracts are preferred (1 > 0)
        - Longer abstracts are preferred (more content)
        - Abstracts without abstract-type are preferred (full abstracts)
        """
        lang = get_xml_lang(node)
        is_en = bool(lang) and lang.lower() == "en"
        text = stringify(node, delimiter=" ") or ""
        has_no_abstract_type = "abstract-type" not in node.attrib
        return (
            int(is_en),              # Prefer English abstracts
            len(text),               # Prefer longer abstracts
            int(has_no_abstract_type),  # Prefer full abstracts (no type attribute)
        )

    # Filter out nodes that have no text at all (score[2] == 0 means has abstract-type)
    # We require at least no abstract-type attribute to be considered
    scored_nodes = []
    for node in nodes:
        s = score_node(node)
        if s[2] > 0:  # Only consider nodes without abstract-type
            scored_nodes.append((s, node))

    if not scored_nodes:
        return ""

    # Sort by score (tuple comparison: English > length > no-type)
    # Reverse=True means highest scores first
    scored_nodes.sort(reverse=True, key=lambda x: x[0])
    best_node = scored_nodes[0][1]
    return stringify(best_node, delimiter=" ")


def extract_pub_date(x: str | etree.ElementTree) -> Date:
    """Extract publication date.
    
    Compared to titipata/pubmed_parser:
    - Extracts year, month, and day
    - Handles month names
    - Applies sanity checks on value validity
    - Validates output using PubDate schema

    For more information, see
    - https://jats.nlm.nih.gov/archiving/tag-library/1.4/element/pub-date.html
    - https://jats.nlm.nih.gov/archiving/tag-library/1.4/attribute/pub-type.html
    
    Args:
        x: Path to XML file or XML tree
        
    Returns:
        PubDate model instance with validated date fields
        
    Raises:
        ValueError: If extracted date data doesn't meet schema validation requirements
    """
    root = get_xml_root(x)

    def score_node(node: etree.Element) -> bool:
        """Score pub-date node to prefer electronic publication dates.
        
        Returns True if this is an epub date or electronic publication date.
        These are preferred over print publication dates.
        """
        is_epub = node.attrib.get("pub-type") == "epub" \
            or (node.attrib.get("publication-format") == "electronic" and node.attrib.get("date-type") == "pub")
        return is_epub 

    # Find all publication date nodes
    nodes = root.xpath("front/article-meta/pub-date")
    if not nodes:
        return Date(year=None, month=None, day=None)

    nodes.sort(key=score_node, reverse=True)
    best_node = nodes[0]
    try:
        year = best_node.find("year").text
    except (AttributeError, ValueError, AssertionError):
        year = None
    
    try:
        month = best_node.find("month").text
        if month.isalpha():
            month = str(convert_month_name_to_number(month))
    except (AttributeError, ValueError, KeyError, AssertionError):
        month = None
    
    try:
        day = best_node.find("day").text
    except (AttributeError, ValueError, AssertionError):
        day = None

    return Date(
        year=year,
        month=month,
        day=day,
    )


def extract_references(x: str | etree.ElementTree) -> list[Reference]:
    """Extract references from article.
    
    Compared to titipata/pubmed_parser:
    - Preserves label information
    - Takes all available pub ids per reference
    - Skips the often poorly formatted information like author names, journal stuff
    - Validates each reference using Reference schema
    
    Args:
        x: Path to XML file or XML tree
        
    Returns:
        list of validated Reference model instances
        
    Raises:
        ValueError: If any extracted reference doesn't meet schema validation requirements
    """
    def score_node(node: etree.Element) -> tuple[bool, bool]:
        """Score reference citation node to determine best one.
        
        Returns tuple of (is_english, is_element_citation).
        Prefers English element-citation nodes over other citation types.
        Tuple comparison ensures English > non-English, and element-citation > others.
        """
        is_element_citation = node.tag == "element-citation"
        lang = get_xml_lang(node)
        is_en = bool(lang) and lang.lower() == "en"
        return is_en, is_element_citation

    # Construct tree and get root element
    root = get_xml_root(x)

    # Find all reference nodes that have ID attributes
    # Using xpath to filter for ref[@id] ensures we only process valid references
    nodes = root.xpath("back/ref-list/ref[@id]")

    # Gather references from this tree
    references: list[Reference] = []
    seen_ref_ids: set[str] = set()
    for ref_idx, ref_node in enumerate(nodes):
        ref_id = ref_node.attrib["id"]

        # Skip if ref id is invalid (not a string or empty)
        if not isinstance(ref_id, str) or not ref_id:
            logger.warning(
                "Non-string or empty ref_id=%r; source=%s; skipping",
                ref_id,
                describe_source(x),
            )
            continue
        # Skip duplicate reference IDs (shouldn't happen, but handle gracefully)
        if ref_id in seen_ref_ids:
            logger.warning("Duplicate ref_id=%r; source=%s; skipping", ref_id, describe_source(x))
            continue
        seen_ref_ids.add(ref_id)

        # Extract reference label (e.g., "1", "2", "a", "b", etc.)
        try:
            ref_label = ref_node.find("label").text
        except (AttributeError, ValueError, AssertionError):
            ref_label = None

        # Initialize reference dictionary with basic info
        reference = {
            "ref_idx": ref_idx,
            "ref_id": ref_id,
            "label": ref_label,
        }

        # Reference citation info is stored in child nodes with "-citation" in tag name
        # Examples: element-citation, mixed-citation, etc.
        # We want to find the best one (prefer English element-citation)
        ref_info_nodes = ref_node.xpath(".//*[contains(local-name(), '-citation')]")
        if ref_info_nodes:
            # Sort by score (English element-citation preferred)
            ref_info_nodes.sort(key=score_node, reverse=True)
            ref_info_node = ref_info_nodes[0]

            # Extract all available referenced IDs (DOI, PMC, PMID, etc.)
            referenced_ids = {}
            referenced_id_nodes = ref_info_node.findall("pub-id")
            for referenced_id_node in referenced_id_nodes:
                referenced_id_type = referenced_id_node.attrib.get("pub-id-type")
                referenced_id = referenced_id_node.text
                # Add pub-id-type as a key in the reference dict (e.g., reference["doi"] = "...")
                if referenced_id_type and referenced_id:
                    referenced_ids[referenced_id_type] = referenced_id
            reference["referenced_ids"] = ArticleIDs(**referenced_ids)

        # Validate and create Reference model instance
        try:
            reference = Reference(**reference)
            references.append(reference)
        except Exception as e:
            # Log validation errors but continue processing other references
            logger.warning(
                "Validation error for reference ref_id=%r; source=%s; skipping. error=%s",
                ref_id,
                describe_source(x),
                e,
            )
            continue
    
    return references


def extract_article(x: str | etree.ElementTree, *, strict: bool = True) -> Article:
    """Extract complete article metadata from PMC XML.
    
    This is a convenience function that calls all extraction functions and
    combines the results into a single Article model instance.
    
    Args:
        x: Path to XML file or XML tree
        
    Returns:
        Article model instance with all extracted and validated metadata
        
    Raises:
        ValueError: If any extracted data doesn't meet schema validation requirements
    """
    try:
        x = build_xml_tree(x)
    except Exception as e:
        print(f"Failed to build XML tree for {describe_source(x)}")
        return x

    def _get(field: str, fn, default):
        try:
            return fn(x)
        except Exception as e:
            if strict:
                raise ValueError(f"Failed to extract {field} from {describe_source(x)}") from e
            logger.warning(
                "Failed to extract %s; source=%s; using default. error=%s",
                field,
                describe_source(x),
                e,
            )
            return default

    article_data = {
        "article_type": _get("article_type", extract_article_type, None),
        "article_ids": _get("article_ids", extract_article_ids, None),
        "subjects": _get("subjects", extract_subjects, []),
        "title": _get("title", extract_title, None),
        "subtitle": _get("subtitle", extract_subtitle, None),
        "abstract": _get("abstract", extract_abstract, None),
        "pub_date": _get("pub_date", extract_pub_date, None),
        "references": _get("references", extract_references, []),
    }

    try:
        return Article(**article_data)
    except Exception as e:
        raise ValueError(f"Failed to validate Article for {describe_source(x)}") from e

