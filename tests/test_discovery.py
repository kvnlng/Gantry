import unittest
from gantry.discovery import ZoneDiscoverer

class TestZoneDiscoverer(unittest.TestCase):
    
    def test_overlap_logic(self):
        # [x, y, w, h]
        b1 = [0, 0, 10, 10]
        
        # Overlap
        b2 = [5, 5, 10, 10] # Starts inside b1
        self.assertTrue(ZoneDiscoverer._boxes_overlap(b1, b2), "Boxes should overlap")
        
        # No overlap (Right)
        b3 = [20, 0, 10, 10]
        self.assertFalse(ZoneDiscoverer._boxes_overlap(b1, b3), "Boxes should NOT overlap")
        
        # Touching (Should be false or true depending on strictness? Code says strict inequality)
        # r1 < l2: 10 < 10 is False.
        # r1 is b1[0]+b1[2] = 10.
        # l2 is 10.
        # 10 < 10 False.
        # Logic is `not (r1 < l2 ...)`
        # If they touch, `r1 == l2`, so `r1 < l2` is MATCH False.
        # So they overlap.
        b4 = [10, 0, 10, 10]
        self.assertTrue(ZoneDiscoverer._boxes_overlap(b1, b4), "Touching boxes should overlap (merge edge)")

    def test_merge_simple(self):
        # Two overlapping boxes
        boxes = [
            [0, 0, 10, 10], 
            [5, 5, 10, 10]  # Ends at 15, 15
        ]
        merged = ZoneDiscoverer._merge_overlapping_boxes(boxes)
        self.assertEqual(len(merged), 1)
        # Union: 0,0 to 15,15 -> [0, 0, 15, 15]
        self.assertEqual(merged[0], [0, 0, 15, 15])

    def test_merge_transitive(self):
        # A overlaps B, B overlaps C, A does NOT overlap C directly
        # A: [0,0,10,10]
        # B: [5,0,10,10] (extends to 15)
        # C: [14,0,10,10] (Starts at 14, overlaps B which ends at 15)
        
        boxes = [
            [0, 0, 10, 10],
            [5, 0, 10, 10],
            [14, 0, 10, 10]
        ]
        
        merged = ZoneDiscoverer._merge_overlapping_boxes(boxes)
        self.assertEqual(len(merged), 1, "Should merge transitively into one")
        # Union: 0 to 24 (14+10)
        self.assertEqual(merged[0], [0, 0, 24, 10])

    def test_merge_disjoint(self):
        # Two separate groups
        boxes = [
            [0, 0, 10, 10],
            [5, 0, 10, 10],
            
            [100, 100, 10, 10],
            [105, 105, 10, 10]
        ]
        
        merged = ZoneDiscoverer._merge_overlapping_boxes(boxes)
        self.assertEqual(len(merged), 2)
        # Order not guaranteed, sort by x to check
        merged.sort(key=lambda x: x[0])
        
        self.assertEqual(merged[0], [0, 0, 15, 10])
        self.assertEqual(merged[1], [100, 100, 15, 15])

    def test_empty(self):
        self.assertEqual(ZoneDiscoverer._merge_overlapping_boxes([]), [])

if __name__ == '__main__':
    unittest.main()
