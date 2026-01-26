import numpy as np
from typing import List, Optional, Any
import logging
from gantry.entities import Instance
from gantry.privacy import PhiFinding

logger = logging.getLogger(__name__)

try:
    import pytesseract
    from PIL import Image
    HAS_OCR = True
except ImportError:
    HAS_OCR = False
    logger.warning("pytesseract or PIL not installed. OCR features will be disabled.")


from dataclasses import dataclass
from typing import List, Optional, Any, Tuple, Dict
import logging
from gantry.entities import Instance

logger = logging.getLogger(__name__)

try:
    import pytesseract
    from PIL import Image
    HAS_OCR = True
except ImportError:
    HAS_OCR = False
    logger.warning("pytesseract or PIL not installed. OCR features will be disabled.")


@dataclass
class TextRegion:
    text: str
    box: Tuple[int, int, int, int]  # x, y, w, h
    confidence: float
    frame_index: int = 0


def detect_text_regions(pixel_data: np.ndarray, frame_idx: int = 0) -> List[TextRegion]:
    """
    Runs OCR on the provided pixel data and returns text regions with bounding boxes.

    Args:
        pixel_data (np.ndarray): The image data.
        frame_idx (int): The frame index associated with this data (for reporting).

    Returns:
        List[TextRegion]: Detected text regions.
    """
    regions = []
    if not HAS_OCR:
        return regions

    try:
        # Normalize pixel types for PIL
        if pixel_data.dtype != np.uint8:
            p_min = pixel_data.min()
            p_max = pixel_data.max()
            if p_max > p_min:
                norm = ((pixel_data - p_min) / (p_max - p_min)) * 255.0
                img_data = norm.astype(np.uint8)
            else:
                img_data = np.zeros(pixel_data.shape, dtype=np.uint8)
        else:
            img_data = pixel_data

        img = Image.fromarray(img_data)
        
        # Use image_to_data for detailed box info
        # config optimized for sparse text
        config = r'--oem 3 --psm 11'
        
        # Output is a dict with lists: level, page_num, block_num, par_num, line_num, word_num, left, top, width, height, conf, text
        data = pytesseract.image_to_data(img, config=config, output_type=pytesseract.Output.DICT)
        
        n_boxes = len(data['text'])
        for i in range(n_boxes):
            text = data['text'][i].strip()
            conf = int(data['conf'][i])
            
            # Filter low confidence and empty text
            if conf > 0 and len(text) > 0:
                (x, y, w, h) = (data['left'][i], data['top'][i], data['width'][i], data['height'][i])
                regions.append(TextRegion(
                    text=text,
                    box=(x, y, w, h),
                    confidence=float(conf),
                    frame_index=frame_idx
                ))
                
    except Exception as e:
        logger.error(f"OCR failed: {e}")
        
    return regions


def detect_text(pixel_data: np.ndarray) -> str:
    """Legacy wrapper for simple string return."""
    regions = detect_text_regions(pixel_data)
    return " ".join([r.text for r in regions])


def analyze_pixels(instance: Instance) -> List[TextRegion]:
    """
    Analyzes the pixel data of a DICOM Instance for burned-in text.
    Returns list of TextRegion objects (raw findings, not filtered).
     Caller is responsible for filtering against rules.
    """
    all_regions = []
    
    if not HAS_OCR:
        return all_regions

    try:
        pixel_array = instance.get_pixel_data()
        if pixel_array is None:
            return all_regions

        frames = []
        shape = pixel_array.shape
        
        # Heuristic to detect frames vs RGB (Same as before)
        if pixel_array.ndim == 2:
            frames.append(pixel_array)
        elif pixel_array.ndim == 3:
            if pixel_array.shape[-1] in [3, 4]:
                frames.append(pixel_array)
            else:
                for i in range(shape[0]):
                    frames.append(pixel_array[i])
        elif pixel_array.ndim == 4:
             for i in range(shape[0]):
                    frames.append(pixel_array[i])
                    
        for i, frame in enumerate(frames):
            regions = detect_text_regions(frame, frame_idx=i)
            all_regions.extend(regions)
                 
    except Exception as e:
        logger.error(f"Failed to analyze pixels for {instance.sop_instance_uid}: {e}")
        
    return all_regions
