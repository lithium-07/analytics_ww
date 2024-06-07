from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class EventPayload(BaseModel):
    platform: Optional[str] = Field(None, example="web")
    event_time: datetime = Field(None, example="2023-05-29T12:34:56.789Z")
    event_name: str = Field(None, example="checkout_complete")
    event_id: str = Field(None, example="evt_1234")
    user_id: Optional[str] = Field(None, example="usr_5678")
    session_id: Optional[str] = Field(None, example="sess_91011")
    page_url: Optional[str] = Field(None, example="https://www.example.com/checkout")
    order_id: Optional[str] = Field(None, example="ord_121314")
    order_value: Optional[float] = Field(None, example=99.99)
    city: Optional[str] = Field(None, example="New York")
    country: Optional[str] = Field(None, example="USA")
    user_agent: Optional[str] = Field(None, example="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    language: Optional[str] = Field(None, example="en-US")
    currency: Optional[str] = Field(None, example="USD")
    user_email: Optional[str] = Field(None, example="user@example.com")
    percent_scroll: Optional[float] = Field(None, example=75)

class MetricsRequest(BaseModel):
    page_url: str
    start_date: str
    end_date: str

class MetricsResponse(BaseModel):
    page_url: str
    cart_percentage: float
    conversion_rate: float
    average_order_value: float
    revenue_per_session: float
    total_sessions: int
    average_scroll_percentage: float