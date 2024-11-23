from datetime import datetime

class ValidationUtils:
    @staticmethod
    def validate_vitals_data(data):
        required_fields = ["patient_id", "timestamp"]
        numeric_fields = ["heart_rate", "blood_pressure_systolic", "blood_pressure_diastolic", 
                         "temperature", "oxygen_saturation", "respiratory_rate"]
        
        # Check required fields
        for field in required_fields:
            if field not in data:
                return False, f"Missing required field: {field}"
                
        # Validate timestamp format
        try:
            datetime.fromisoformat(data["timestamp"].replace('Z', '+00:00'))
        except ValueError:
            return False, "Invalid timestamp format"
            
        # Validate numeric fields
        for field in numeric_fields:
            if field in data and not isinstance(data[field], (int, float)):
                return False, f"Invalid numeric value for {field}"
                
        return True, "Valid data"
