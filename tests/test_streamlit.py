from streamlit.testing.v1 import AppTest

def test_app_renders():
    at = AppTest.from_file("app.py").run()

    assert not at.exception