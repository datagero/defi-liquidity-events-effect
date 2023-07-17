def merge_nested_dicts(dict1, dict2):
    """
    Merges two nested dictionaries into a single dictionary.
    If the same key exists in both dictionaries, the value from dict2 is kept.
    Args:
        dict1, dict2 (dict): The dictionaries to merge.
    Returns:
        merged_dict (dict): The merged dictionary.
    """
    merged_dict = dict1.copy()
    for key, value in dict2.items():
        if key in merged_dict and isinstance(merged_dict[key], dict) and isinstance(value, dict):
            merged_dict[key] = merge_nested_dicts(merged_dict[key], value)
        else:
            merged_dict[key] = value
    return merged_dict