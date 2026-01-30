"""
Module for discovering common locations of burned-in text (hotspots) in DICOM instances.
"""
import logging
import re
from typing import List, Tuple, Any, Dict, Union

from gantry.entities import Instance
from gantry.pixel_analysis import analyze_pixels

logger = logging.getLogger(__name__)

from dataclasses import dataclass
from typing import List, Dict, Any, Optional

@dataclass
class DiscoveryCandidate:
    """A single text region detected during discovery."""
    text: str
    confidence: float
    box: List[int] # [x, y, w, h]
    source_index: int
    classification: str

class DiscoveryResult:
    """
    Holds the raw results of a discovery scan and provides methods to filter 
    and group them into actionable redaction zones.
    """
    def __init__(self, candidates: List[DiscoveryCandidate], n_sources: int):
        self.candidates = candidates
        self.n_sources = n_sources

    def __len__(self):
        return len(self.candidates)

    def filter(self, min_confidence: float = 0.0) -> 'DiscoveryResult':
        """Returns a new result with filtered candidates."""
        filtered = [c for c in self.candidates if c.confidence >= min_confidence]
        return DiscoveryResult(filtered, self.n_sources)

    def to_zones(self, pad_x: int = 20, pad_y: int = 10, min_occurrence: float = 0.1) -> List[Dict[str, Any]]:
        """
        Groups candidates into suggested redaction zones.
        
        Args:
            pad_x: Horizontal padding for merging (higher values merge words on same line).
            pad_y: Vertical padding.
            min_occurrence: Fraction of valid sources (0.0-1.0) required to suggest a zone.
        """
        if not self.candidates:
            return []

        # 1. Prepare boxes for clustering
        boxes = [c.box for c in self.candidates]
        
        # 2. Cluster
        clusters = ZoneDiscoverer.group_boxes(boxes, pad_x=pad_x, pad_y=pad_y)
        
        final_zones = []
        
        for cluster_indices in clusters:
            subset = [self.candidates[i] for i in cluster_indices]
            
            # Geometry Union
            merged_box = ZoneDiscoverer._union_box_list([c.box for c in subset])
            
            # Frequency Check
            unique_sources = {c.source_index for c in subset}
            occurrence_rate = len(unique_sources) / self.n_sources
            
            if occurrence_rate < min_occurrence:
                continue
                
            # Aggregated Metadata
            texts = list({c.text for c in subset}) # Dedup
            
            # Smart Type Classification based on the group
            zone_type = "TEXT"
            if any(c.classification == "NAME_PATTERN" for c in subset):
                zone_type = "LIKELY_NAME"
            elif any(c.classification == "PROPER_NOUN" for c in subset):
                zone_type = "PROPER_NOUN"
            
            # Average Confidence
            avg_conf = sum(c.confidence for c in subset) / len(subset)

            # Filter tiny zones
            if merged_box[2] > 5 and merged_box[3] > 5:
                # Convert to [y1, y2, x1, x2] for Gantry Config
                x, y, w, h = merged_box
                final_zones.append({
                    "zone": [y, y + h, x, x + w],
                    "type": zone_type,
                    "occurrence": occurrence_rate,
                    "confidence": avg_conf,
                    "examples": texts[:3]
                })
                
        return final_zones

class ZoneDiscoverer:
    """
    Utilities for analyzing text regions and classifying them.
    """
    _nlp_model = None
    _nlp_model_failed = False

    @staticmethod
    def _classify_text(text: str) -> str:
        """
        Classifies text as 'NAME_PATTERN', 'PROPER_NOUN', or 'TEXT'.
        """
        clean = text.strip()
        if not clean:
            return "TEXT"
            
        # 1. Explicit DICOM Name Pattern
        if '^' in clean and any(c.isalpha() for c in clean):
            return "NAME_PATTERN"
            
        # 2. NLP (spaCy) - Optional
        if not ZoneDiscoverer._nlp_model_failed:
            if ZoneDiscoverer._nlp_model is None:
                try:
                    import spacy
                    try:
                        ZoneDiscoverer._nlp_model = spacy.load("en_core_web_sm")
                        logger.info("Loaded spaCy NLP model.")
                    except OSError:
                        logger.warning("spaCy model 'en_core_web_sm' not found.")
                        ZoneDiscoverer._nlp_model_failed = True
                except ImportError:
                    ZoneDiscoverer._nlp_model_failed = True
        
        if ZoneDiscoverer._nlp_model:
            doc = ZoneDiscoverer._nlp_model(clean)
            for ent in doc.ents:
                if ent.label_ in ("PERSON", "ORG"): # Accept ORG too
                    return "PROPER_NOUN"
            
        # 3. Fallback Heuristic
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
    def group_boxes(boxes: List[List[int]], padding: int = 0, pad_x: int = None, pad_y: int = None) -> List[List[int]]:
        """Groups boxes into overlapping clusters."""
        if not boxes:
            return []
        
        px = pad_x if pad_x is not None else padding
        py = pad_y if pad_y is not None else padding
        
        n = len(boxes)
        adj = [[] for _ in range(n)]

        for i in range(n):
            for j in range(i + 1, n):
                if ZoneDiscoverer._boxes_overlap(boxes[i], boxes[j], px, py):
                    adj[i].append(j)
                    adj[j].append(i)

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
        if not boxes:
            return [0, 0, 0, 0]
        ux, uy, uw, uh = boxes[0]
        ur, ub = ux + uw, uy + uh
        for k in range(1, len(boxes)):
            b = boxes[k]
            ux = min(ux, b[0])
            uy = min(uy, b[1])
            ur = max(ur, b[0] + b[2])
            ub = max(ub, b[1] + b[3])
        return [ux, uy, ur - ux, ub - uy]

    @staticmethod
    def _boxes_overlap(b1, b2, px: int, py: int) -> bool:
        l1, t1, r1, b1_ = b1[0], b1[1], b1[0]+b1[2], b1[1]+b1[3]
        l2, t2, r2, b2_ = b2[0], b2[1], b2[0]+b2[2], b2[1]+b2[3]
        return not ((r1 + px) < l2 or (l1 - px) > r2 or (b1_ + py) < t2 or (t1 - py) > b2_)

    @staticmethod
    def _merge_overlapping_boxes(boxes: List[List[int]], padding: int = 0) -> List[List[int]]:
        """Legacy helper."""
        clusters = ZoneDiscoverer.group_boxes(boxes, padding=padding)
        return [ZoneDiscoverer._union_box_list([boxes[i] for i in cluster]) for cluster in clusters]
