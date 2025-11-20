from config import (
    FRANCHISE_GROUPS,
    PLANFIX_IT_TEMPLATES,
    PLANFIX_SE_TEMPLATES,
    get_available_templates,
    get_template_info,
)


def test_franchise_groups_structure():
    assert isinstance(FRANCHISE_GROUPS, dict)
    assert FRANCHISE_GROUPS, "Expected at least one franchise group"

    for group_id, data in FRANCHISE_GROUPS.items():
        assert isinstance(group_id, int)
        assert "name" in data and isinstance(data["name"], str) and data["name"]
        # Контакты теперь получаются из Planfix через API, а не хранятся в конфиге


def test_templates_registry():
    templates = {
        **PLANFIX_SE_TEMPLATES,
        **PLANFIX_IT_TEMPLATES,
    }
    assert templates, "Registry must not be empty"
    expected_ids = {83454, 80839}
    assert expected_ids.issubset(templates.keys())

    available = get_available_templates(16, 251)
    returned_ids = {tpl["id"] for tpl in available}
    assert expected_ids.issubset(returned_ids)

    tpl = get_template_info(83454)
    assert tpl is not None
    assert tpl["name"] == "Служба Эксплуатации"

