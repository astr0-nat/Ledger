from datetime import datetime
import yaml
import re
import logging.config

from logging_config import LOGGING_CONFIG

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def parse_date(date_str):
    for date_format in ("%m/%d/%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            date = datetime.strptime(date_str, date_format)
            return date
        except ValueError:
            continue
        except Exception as e:
            logger.error(f"Error in parsing date {date_str}: {e}", exc_info=True)
    return None


def remove_ruby_yaml_tags_and_quote_special_chars(yaml_str):
    # Remove Ruby-specific tags
    yaml_str = re.sub(r'!ruby/[^ \n]*', '', yaml_str)

    # Convert the string to lines for processing
    lines = yaml_str.split('\n')
    processed_lines = []

    for line in lines:
        # Skip empty lines
        if not line.strip():
            processed_lines.append(line)
            continue

        # Try to split into key and value
        match = re.match(r'^(\s*)(\w+):\s*(.*)$', line)
        if match:
            # Extract indentation, key, and value while preserving structure
            indent, key, value = match.groups()
            value = value.strip()

            # Check if value needs quoting
            if value and any(char in value for char in ['&', '*', '#', '?', '|', '>', '!', '%', '@', '`', '\'', '"']):
                if not (value.startswith('"') and value.endswith('"')) and not (
                        value.startswith("'") and value.endswith("'")):
                    value = f'"{value}"'

            processed_lines.append(f"{indent}{key}: {value}")
        else:
            # Keep lines that don't match the pattern (like nested structures)
            processed_lines.append(line)

    return '\n'.join(processed_lines)


def yaml_load(content):
    """
    Load YAML content after removing Ruby tags and quoting special characters.
    """
    content = remove_ruby_yaml_tags_and_quote_special_chars(content)
    try:
        data = yaml.safe_load(content)
        # Ensure nested structures are dictionaries
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, str) and ':' in value:
                    try:
                        nested_data = yaml.safe_load(value)
                        if isinstance(nested_data, dict):
                            data[key] = nested_data
                    except yaml.YAMLError:
                        pass
        return data or {}
    except yaml.YAMLError as e:
        logger.error(f"Error during YAML loading for content {content}: {e}")
        return {}


def yaml_dump(data):
    """
    Dump data to YAML format.
    """
    return yaml.dump(data, default_flow_style=False)


def yaml_dump_with_ruby_tags(data):
    """
    Custom YAML dump function that includes Ruby-specific type tags.
    Necessary for Rails to properly read.
    """
    lines = ["--- !ruby/hash:ActiveSupport::HashWithIndifferentAccess"]
    for key, value in data.items():
        if key == 'bill' and isinstance(value, dict):
            lines.append(f"bill: !ruby/hash:ActiveSupport::HashWithIndifferentAccess")
            for sub_key, sub_value in value.items():
                # Ensure proper indentation for nested keys
                lines.append(f"  {sub_key}: '{sub_value}'")
        else:
            lines.append(f"{key}: '{value}'")
    return "\n".join(lines) + "\n"
