"""
Module for discovering common locations of burned-in text (hotspots) in DICOM instances.
"""
import logging
from typing import List
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
        scans instances and returns a list of suggested zones [x, y, w, h].
        
        Args:
            instances: List of DICOM instances to scan.
            min_occurrence: Fraction of instances that must contain text in a region
                            for it to be considered a "zone" (Not strictly used in MVP).
        
        Returns:
            List[List[int]]: List of suggested zones [y1, y2, x1, x2].
        """
        all_regions = []
        
        for inst in instances:
            regions = analyze_pixels(inst)
            all_regions.extend(regions)
            
        if not all_regions:
            return []
            
        # Convert to working format
        boxes = [list(r.box) for r in all_regions] # [[x,y,w,h], ...]
        
        merged_boxes = ZoneDiscoverer._merge_overlapping_boxes(boxes)
        
        final_zones = []
        for box in merged_boxes:
            if box[2] > 5 and box[3] > 5: # Min size 5x5
                # Convert [x, y, w, h] -> [y1, y2, x1, x2]
                x, y, w, h = box
                final_zones.append([y, y + h, x, x + w])
                 
        return final_zones

    @staticmethod
    def _merge_overlapping_boxes(boxes: List[List[int]]) -> List[List[int]]:
        """
        Iteratively merges overlapping boxes until no overlaps remain.
        """
        if not boxes:
            return []

        n = len(boxes)
        
        # Build adjacency graph (indices)
        adj = [[] for _ in range(n)]
        
        # O(N^2) comparison - fine for N < ~2000
        for i in range(n):
            for j in range(i + 1, n):
                if ZoneDiscoverer._boxes_overlap(boxes[i], boxes[j]):
                    adj[i].append(j)
                    adj[j].append(i)
        
        # Find connected components (BFS)
        visited = [False] * n
        merged_results = []
        
        for i in range(n):
            if not visited[i]:
                # Start new component
                visited[i] = True
                component = [boxes[i]]
                queue = [i]
                
                while queue:
                    curr = queue.pop(0)
                    for neighbor in adj[curr]:
                        if not visited[neighbor]:
                            visited[neighbor] = True
                            component.append(boxes[neighbor])
                            queue.append(neighbor)
                
                # Merge component
                # Start with first box
                ux, uy, uw, uh = component[0]
                ur = ux + uw
                ub = uy + uh
                
                for k in range(1, len(component)):
                    b = component[k]
                    ux = min(ux, b[0])
                    uy = min(uy, b[1])
                    ur = max(ur, b[0] + b[2])
                    ub = max(ub, b[1] + b[3])
                    
                merged_results.append([ux, uy, ur - ux, ub - uy])
                
        return merged_results

    @staticmethod
    def _boxes_overlap(b1, b2) -> bool:
        # b = [x, y, w, h]
        l1, t1, r1, b1_ = b1[0], b1[1], b1[0]+b1[2], b1[1]+b1[3]
        l2, t2, r2, b2_ = b2[0], b2[1], b2[0]+b2[2], b2[1]+b2[3]
        
        return not (r1 < l2 or l1 > r2 or b1_ < t2 or t1 > b2_)
