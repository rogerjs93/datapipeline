from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class Demographics(BaseModel):
    patient_id: str = Field(..., description="Unique patient identifier")
    birth_date: Optional[date] = Field(None, description="Date of birth (YYYY-MM-DD)")
    sex: Optional[str] = Field(None, description="M/F/Other/Unknown")
    race: Optional[str] = Field(None)
    ethnicity: Optional[str] = Field(None)
    zip_code: Optional[str] = Field(None)


class Vitals(BaseModel):
    patient_id: str
    timestamp: datetime
    heart_rate: Optional[float]
    systolic_bp: Optional[float]
    diastolic_bp: Optional[float]
    respiratory_rate: Optional[float]
    temperature: Optional[float]


class LabRecord(BaseModel):
    patient_id: str
    timestamp: datetime
    test_name: str
    loinc_code: Optional[str]
    value: Optional[float]
    unit: Optional[str]
    reference_range: Optional[str]
