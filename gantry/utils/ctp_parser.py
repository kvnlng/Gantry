
import re

class CTPParser:
    """
    Parses CTP DicomPixelAnonymizer.script files into Gantry-compatible rules.
    """

    @staticmethod
    def parse_script(content: str):
        rules = []
        
        # Simple finite state machine or regex approach
        # The format is roughly:
        # Title/Comment (Lines)
        # { condition }
        # (x,y,w,h) ...
        
        # Regex to find blocks of { condition } followed by coordinates
        # Conditions might span multiple lines.
        # Coordinates might span multiple lines
        
        # Normalize whitespace
        content = content.replace('\r\n', '\n')
        
        # Split by blocks?
        # Let's try to match the pattern:
        # { ... }
        # ( ... )
        
        # Regex for condition block
        pattern = re.compile(r'\{\s*(.*?)\s*\}\s*([\(\)\d\s,]+)', re.DOTALL)
        
        matches = pattern.findall(content)
        
        for condition_str, coords_str in matches:
            rule = CTPParser._parse_block(condition_str, coords_str)
            if rule:
                rules.append(rule)
                
        return rules

    @staticmethod
    def _parse_block(condition_str, coords_str):
        # 1. Parse Condition to extract Match Criteria
        criteria = {}
        
        # Examples: 
        # Manufacturer.containsIgnoreCase("GE MEDICAL")
        # ManufacturerModelName.containsIgnoreCase("Aquilion ONE")
        # Rows.equals("512")
        
        # Extract Manufacturer
        m_man = re.search(r'Manufacturer\.containsIgnoreCase\("([^"]+)"\)', condition_str)
        if m_man:
            criteria['manufacturer'] = m_man.group(1)
            
        # Extract Model
        # ManufacturerModelName can be mapped to model_name
        m_mod = re.search(r'ManufacturerModelName\.containsIgnoreCase\("([^"]+)"\)', condition_str)
        if m_mod:
            criteria['model_name'] = m_mod.group(1)
            
        # Extract Serial Number (if available? CTP scripts usually target Modality/Model, rarely serial)
        # But we can look for it.
        
        # If no manufacturer/model, it might be generic.
        if not criteria:
            # Maybe extract others for comment?
            pass
            
        # 2. Parse Coordinates
        # (x,y,w,h)
        # Gantry expects: [r1, r2, c1, c2] = [y, y+h, x, x+w]
        
        gantry_zones = []
        
        # Find all (x,y,w,h) tuples
        coord_matches = re.findall(r'\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)', coords_str)
        
        for (x, y, w, h) in coord_matches:
            x, y, w, h = int(x), int(y), int(w), int(h)
            gantry_zone = [y, y+h, x, x+w]
            gantry_zones.append(gantry_zone)
            
        if not gantry_zones:
            return None
            
        # Build Rule Object
        # If we have extracted specific info, use it.
        # Ideally we want key info for matching.
        
        if 'manufacturer' in criteria or 'model_name' in criteria:
             return {
                 "manufacturer": criteria.get("manufacturer", "Unknown"),
                 "model_name": criteria.get("model_name", "Unknown"),
                 "comment": f"Imported from CTP. Condition: {condition_str.strip()[:100]}...",
                 "redaction_zones": gantry_zones,
                 # Store raw condition for debugging?
                 "_ctp_condition": condition_str.strip()
             }
        
        return None
