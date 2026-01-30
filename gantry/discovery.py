"""
Module for discovering common locations of burned-in text (hotspots) in DICOM instances.
"""
import logging
import re
from typing import List, Tuple, Any, Dict, Union

from gantry.entities import Instance
from gantry.pixel_analysis import analyze_pixels

logger = logging.getLogger(__name__)

class ZoneDiscoverer:
    """
    Analyzes a set of instances to discover common locations of burned-in text.
    Used to suggest initial redaction zones for a machine.
    """
    _nlp_model = None
    _nlp_model_failed = False
    @staticmethod
    @staticmethod
    def _classify_text(text: str) -> str:
        """
        Classifies text as 'NAME_PATTERN', 'PROPER_NOUN', or 'TEXT'.
        Uses explicit regex patterns first, then falls back to NLP if available.
        """
        clean = text.strip()
        if not clean:
            return "TEXT"
            
        # 1. Explicit DICOM Name Pattern (Caret)
        # Fast, deterministic, and high confidence for medical images
        if '^' in clean and any(c.isalpha() for c in clean):
            return "NAME_PATTERN"
            
        # 2. NLP (spaCy) - Optional
        # Lazily load and cache the model
        if not ZoneDiscoverer._nlp_model_failed:
            if ZoneDiscoverer._nlp_model is None:
                try:
                    import spacy
                    # "en_core_web_sm" is small and fast logic
                    try:
                        ZoneDiscoverer._nlp_model = spacy.load("en_core_web_sm")
                        logger.info("Loaded spaCy NLP model for better entity detection.")
                    except OSError:
                        # Model not downloaded
                        logger.warning("spaCy installed but 'en_core_web_sm' model not found. "
                                     "Skipping NLP. Run 'python -m spacy download en_core_web_sm' or install gantry[nlp].")
                        ZoneDiscoverer._nlp_model_failed = True
                except ImportError:
                    # spacy not installed
                    ZoneDiscoverer._nlp_model_failed = True
        
        if ZoneDiscoverer._nlp_model:
            doc = ZoneDiscoverer._nlp_model(clean)
            for ent in doc.ents:
                if ent.label_ == "PERSON":
                    return "PROPER_NOUN"
                if ent.label_ == "ORG" and "Hospital" in clean:
                    # Also useful
                    return "PROPER_NOUN"
            
        # 3. Fallback Heuristic: Capitalized Words
        # Check if words start with Upper and have length > 1
        words = clean.split()
        cap_count = 0
        for w in words:
            w_clean = re.sub(r'[^\w\s]', '', w)
            if w_clean and w_clean[0].isupper() and len(w_clean) > 1:
                cap_count += 1
        
        if cap_count > 0:
            return "PROPER_NOUN_CANDIDATE"
            
        return "TEXT"

    @staticmethod
    def discover_zones(instances: List[Instance], _min_occurrence: float = 0.1, min_confidence: float = 80.0) -> List[Dict[str, Any]]:
        """
        Scans instances and returns a list of suggested zones with metadata.

        Args:
            instances: List of DICOM instances to scan.
            _min_occurrence: Fraction of instances that must contain text in a region.
            min_confidence: Minimum OCR confidence (0-100).

        Returns:
            List[Dict]: List of zone dicts: {'zone': [y1, y2, x1, x2], 'type': str, 'text_samples': List[str]}
        """
        # Collect regions with source tracking
        # Each item: (TextRegion, instance_uid)
        tagged_regions = []

        for inst in instances:
            regions = analyze_pixels(inst)
            uid = inst.sop_instance_uid
            for r in regions:
                if r.confidence >= min_confidence:
                    tagged_regions.append((r, uid))

        if not tagged_regions:
            return []
        
        boxes = [list(item[0].box) for item in tagged_regions]

        # Use padding to fix fragmentation
        # Aggressive horizontal padding (100px) to merge "Hospital ... Name"
        # Strict vertical padding (10px) to keep lines separate
        clusters = ZoneDiscoverer.group_boxes(boxes, pad_x=100, pad_y=10)

        final_zones = []
        n_total_instances = len(instances)

        for cluster_indices in clusters:
            # 1. Merge the box geometry
            cluster_boxes = [boxes[i] for i in cluster_indices]
            merged_box = ZoneDiscoverer._union_box_list(cluster_boxes)
            
            # 2. Check occurrence frequency (Noise Filter)
            unique_sources = {tagged_regions[i][1] for i in cluster_indices}
            occurrence_rate = len(unique_sources) / n_total_instances

            # Collect text samples (for classification)
            cluster_texts = [tagged_regions[i][0].text for i in cluster_indices]
            
            # Classify Cluster
            # If ANY text in the cluster is a NAME or PROPER NOUN, label the whole zone.
            cluster_type = "TEXT"
            has_proper_noun = False
            has_name_pattern = False
            
            for t in cluster_texts:
                cls = ZoneDiscoverer._classify_text(t)
                if cls == "NAME_PATTERN":
                    has_name_pattern = True
                elif cls == "PROPER_NOUN_CANDIDATE":
                    has_proper_noun = True
            
            if has_name_pattern:
                cluster_type = "LIKELY_NAME"
            elif has_proper_noun:
                cluster_type = "PROPER_NOUN"

            # Filter Logic:
            # If it's a NAME or PROPER NOUN, be more lenient with occurrence?
            # Or just keep standard logic.
            # If occurrence is low, but it's a NAME, we might want to keep it?
            # But names are unique. If we have 100 instances and 100 unique names, 
            # occurrence of "John" is 1%. "Mary" is 1%.
            # But they are spatially clustered!
            # The CLUSTER occurrence is key.
            # Since names appear in the SAME PLACE, the cluster should contain ALL of them.
            # So occurrence_rate should be 100% (or high).
            # So standard filtering works IF clustering works.
            
            if occurrence_rate < _min_occurrence:
                continue

            # 3. Filter tiny zones
            if merged_box[2] > 5 and merged_box[3] > 5: # Min size 5x5
                x, y, w, h = merged_box
                zone_rect = [y, y + h, x, x + w]
                
                final_zones.append({
                    "zone": zone_rect,
                    "type": cluster_type,
                    "occurrence": occurrence_rate,
                    "examples": list(set(cluster_texts))[:3] # Sample
                })

        return final_zones

    @staticmethod
    def group_boxes(boxes: List[List[int]], padding: int = 0, pad_x: int = None, pad_y: int = None) -> List[List[int]]:
        """
        Groups boxes into overlapping clusters.
        Supports separate x/y padding. If pad_x/pad_y provided, 'padding' is ignored.
        """
        if not boxes:
            return []

        px = pad_x if pad_x is not None else padding
        py = pad_y if pad_y is not None else padding
        
        n = len(boxes)
        adj = [[] for _ in range(n)]

        # O(N^2) comparison
        for i in range(n):
            for j in range(i + 1, n):
                if ZoneDiscoverer._boxes_overlap(boxes[i], boxes[j], px, py):
                    adj[i].append(j)
                    adj[j].append(i)

        # BFS for components
        visited = [False] * n
        clusters = []

        for i in range(n):
            if not visited[i]:
                visited[i] = True
                cluster = [i]
                queue = [i]

                while queue:
                    curr = queue.pop(0)
                    for neighbor in adj[curr]:
                        if not visited[neighbor]:
                            visited[neighbor] = True
                            cluster.append(neighbor)
                            queue.append(neighbor)
                
                clusters.append(cluster)

        return clusters

    @staticmethod
    def _union_box_list(boxes: List[List[int]]) -> List[int]:
        """Calculates the union bounding box of a list of boxes."""
        if not boxes:
            return [0, 0, 0, 0]
            
        ux, uy, uw, uh = boxes[0]
        ur = ux + uw
        ub = uy + uh

        for k in range(1, len(boxes)):
            b = boxes[k]
            ux = min(ux, b[0])
            uy = min(uy, b[1])
            ur = max(ur, b[0] + b[2])
            ub = max(ub, b[1] + b[3])

        return [ux, uy, ur - ux, ub - uy]

        
    @staticmethod
    def _merge_overlapping_boxes(boxes: List[List[int]], padding: int = 0) -> List[List[int]]:
        """
        Legacy/Internal: Merges overlapping boxes and returns the geometry of merged boxes.
        """
        clusters = ZoneDiscoverer.group_boxes(boxes, padding=padding)
        return [ZoneDiscoverer._union_box_list([boxes[i] for i in cluster]) for cluster in clusters]

    @staticmethod
    @staticmethod
    def _boxes_overlap(b1, b2, px: int, py: int) -> bool:
        # b = [x, y, w, h]
        
        l1, t1, r1, b1_ = b1[0], b1[1], b1[0]+b1[2], b1[1]+b1[3]
        l2, t2, r2, b2_ = b2[0], b2[1], b2[0]+b2[2], b2[1]+b2[3]
        
        # Check disjointness with padding tolerance
        is_left = (r1 + px) < l2
        is_right = (l1 - px) > r2
        is_above = (b1_ + py) < t2
        is_below = (t1 - py) > b2_
        
        return not (is_left or is_right or is_above or is_below)
