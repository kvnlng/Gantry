"""
Module for discovering common locations of burned-in text (hotspots) in DICOM instances.
"""
import logging
from typing import List, Tuple, Any

from gantry.entities import Instance
from gantry.pixel_analysis import analyze_pixels

logger = logging.getLogger(__name__)

class ZoneDiscoverer:
    """
    Analyzes a set of instances to discover common locations of burned-in text.
    Used to suggest initial redaction zones for a machine.
    """

    @staticmethod
    def discover_zones(instances: List[Instance], _min_occurrence: float = 0.1) -> List[List[int]]:
        """
        Scans instances and returns a list of suggested zones [y1, y2, x1, x2].

        Args:
            instances: List of DICOM instances to scan.
            _min_occurrence: Fraction of instances that must contain text in a region
                            for it to be considered a "zone".

        Returns:
            List[List[int]]: List of suggested zones [y1, y2, x1, x2].
        """
        # Collect regions with source tracking
        # Each item: (box, instance_uid)
        tagged_boxes = []

        for inst in instances:
            regions = analyze_pixels(inst)
            uid = inst.sop_instance_uid
            for r in regions:
                tagged_boxes.append((list(r.box), uid))

        if not tagged_boxes:
            return []
        
        boxes = [item[0] for item in tagged_boxes]

        # Use padding to fix fragmentation
        # 5 pixels padding allows merging adjacent words
        clusters = ZoneDiscoverer.group_boxes(boxes, padding=5)

        final_zones = []
        n_total_instances = len(instances)

        for cluster_indices in clusters:
            # 1. Merge the box geometry
            cluster_boxes = [boxes[i] for i in cluster_indices]
            merged_box = ZoneDiscoverer._union_box_list(cluster_boxes)
            
            # 2. Check occurrence frequency (Noise Filter)
            # Count unique instances in this cluster
            unique_sources = {tagged_boxes[i][1] for i in cluster_indices}
            occurrence_rate = len(unique_sources) / n_total_instances

            if occurrence_rate < _min_occurrence:
                # Skip noise
                continue

            # 3. Filter tiny zones
            if merged_box[2] > 5 and merged_box[3] > 5: # Min size 5x5
                # Convert [x, y, w, h] -> [y1, y2, x1, x2]
                x, y, w, h = merged_box
                final_zones.append([y, y + h, x, x + w])

        return final_zones

    @staticmethod
    def group_boxes(boxes: List[List[int]], padding: int = 0) -> List[List[int]]:
        """
        Groups boxes into overlapping clusters.
        
        Args:
            boxes: List of [x, y, w, h]
            padding: Pixels to expand check by (to merge close boxes)

        Returns:
            List of Lists of indices. Each inner list contains indices of boxes that form a cluster.
        """
        if not boxes:
            return []

        n = len(boxes)
        adj = [[] for _ in range(n)]

        # O(N^2) comparison
        for i in range(n):
            for j in range(i + 1, n):
                if ZoneDiscoverer._boxes_overlap(boxes[i], boxes[j], padding=padding):
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
    def _boxes_overlap(b1, b2, padding: int = 0) -> bool:
        # b = [x, y, w, h]
        # Expand b1 by padding/2? Or just check distance?
        # Simpler: Expand both "virtual" boxes by padding, check overlap
        
        # Actually, if we want "within 5 pixels", we can expand one by padding.
        # Strict logic:
        # r1 < l2 - padding
        
        l1, t1, r1, b1_ = b1[0], b1[1], b1[0]+b1[2], b1[1]+b1[3]
        l2, t2, r2, b2_ = b2[0], b2[1], b2[0]+b2[2], b2[1]+b2[3]

        # Check disjointness with padding tolerance
        # If r1 is left of l2 by more than padding, they are disjoint
        # ie. r1 + padding < l2
        
        is_left = (r1 + padding) < l2
        is_right = (l1 - padding) > r2
        is_above = (b1_ + padding) < t2
        is_below = (t1 - padding) > b2_
        
        return not (is_left or is_right or is_above or is_below)
