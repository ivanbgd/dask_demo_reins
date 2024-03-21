"""
File:       type_aliases.py
Author:     Ivan Lazarević
Brief:      Type aliases for type annotations.
"""
from typing import Dict, List, Mapping, Optional, Union

# Type aliases
_Attribute = Dict[str, Union[str, List[str]]]
Coverage = List[_Attribute]
Contract = Dict[str, Union[Coverage, int]]

Deal = Mapping[str, Union[List[int], List[str]]]
DealNone = Mapping[str, Union[List[int], List[Optional[int]], List[str], List[Optional[str]]]]
