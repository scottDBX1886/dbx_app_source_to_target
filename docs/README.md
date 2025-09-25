# Documentation

This directory contains documentation, prototypes, and configuration backups for the Gainwell Main App.

## Files

### UI Prototypes
- **`fmt_interactive_prototype.html`** - Interactive FMT prototype
  - Original UI/UX design for FMT module
  - HTML/CSS/JavaScript prototype
  - Reference for field layouts and interactions
  - Note: Final implementation uses FDB-style design instead

### Configuration Backups
- **`settings_backup`** - Backup of original pydantic-settings configuration
  - Contains original settings.py using pydantic-settings
  - Preserved for reference after migration to os.getenv()
  - Includes BaseSettings class and validation logic

## Usage

### View FMT Prototype
Open `fmt_interactive_prototype.html` in a web browser to see the original FMT design mockup.

### Reference Configuration
The `settings_backup` file shows the original configuration approach before simplification.

## Historical Context

### FMT Design Evolution
1. **Original Design**: Based on `fmt_interactive_prototype.html` 
2. **User Feedback**: "Design should match FDB for look and feel"
3. **Final Implementation**: FDB-style UI with FMT-specific data fields

### Configuration Evolution  
1. **Original**: Used `pydantic-settings` with BaseSettings
2. **Simplified**: Direct `os.getenv()` calls for better compatibility
3. **Backup**: Original approach preserved in `settings_backup`

## Related Documentation
- Main project README.md in root directory
- Individual module READMEs in subdirectories
- Sample data documentation in data directories
