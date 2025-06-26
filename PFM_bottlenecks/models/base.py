from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field

class ConfidenceLevel(str, Enum):
    strong = "strong"
    borderline = "borderline"
    weak = "weak"


