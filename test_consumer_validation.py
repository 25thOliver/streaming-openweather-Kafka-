import pytest
from pydantic import ValidationError
from openweather_consumer import WeatherRecord

def test_valid_record():
	data = {
		"city": "Nairobi",
		"timestamp": 1694898000,
		"payload": {"main": {"temp": 22}, "weather": [{"description": "clear sky"}]}
	}
	record = WeatherRecord(**data)
	assert record.city == "Nairobi"
	assert record.timestamp == 1694898000
	assert "main" in record.payload

def test_missing_city():
	data = {
		"timestamp": 1694898000,
        	"payload": {"main": {"temp": 22}, "weather": []}
	}
	with pytest.raises(ValidationError):
		WeatherRecord(**data)

def test_negative_timestamp():
	data = {
		"city": "Paris",
        	"timestamp": -1,
        	"payload": {"main": {"temp": 20}, "weather": []}
	}
	with pytest.raises(ValidationError):
		WeatherRecord(**data)

def test_invalid_payload_type():
	data = {
		"city": "Berlin",
        	"timestamp": 1694898000,
        	"payload": "not-a-dict"
	}
	with pytest.raises(ValidationError):
		WeatherRecord(**data)
