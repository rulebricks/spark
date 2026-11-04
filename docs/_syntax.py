"""Map the reference docs' Nextra syntax colors to Sphinx's Pygments tokens."""

from pygments.style import Style
from pygments.token import (
    Comment,
    Generic,
    Keyword,
    Name,
    Number,
    Operator,
    Punctuation,
    String,
    Text,
)


class RulebricksLight(Style):
    background_color = "#f5f5f2"
    highlight_color = "#eaf9e7"
    styles = {
        Text: "#414141",
        Comment: "#6b7280",
        Keyword: "#d32f2f",
        Name: "#414141",
        Name.Builtin: "#1976d2",
        Name.Constant: "#1976d2",
        Name.Function: "#6f42c1",
        Name.Class: "#6f42c1",
        Name.Decorator: "#6f42c1",
        Number: "#1976d2",
        Operator: "#d32f2f",
        Punctuation: "#212121",
        String: "#22863a",
        Generic.Deleted: "#d32f2f",
        Generic.Inserted: "#22863a",
        Generic.Heading: "bold #414141",
        Generic.Subheading: "bold #414141",
        Generic.Emph: "italic",
        Generic.Strong: "bold",
    }


class RulebricksDark(RulebricksLight):
    background_color = "#1a1a1a"
    highlight_color = "#263e1e"
    styles = {
        Text: "#d1d1d1",
        Comment: "#9ca3af",
        Keyword: "#f97583",
        Name: "#d1d1d1",
        Name.Builtin: "#79b8ff",
        Name.Constant: "#79b8ff",
        Name.Function: "#b392f0",
        Name.Class: "#b392f0",
        Name.Decorator: "#b392f0",
        Number: "#79b8ff",
        Operator: "#f97583",
        Punctuation: "#bbbbbb",
        String: "#ffab70",
        Generic.Deleted: "#f97583",
        Generic.Inserted: "#4bb74a",
        Generic.Heading: "bold #d1d1d1",
        Generic.Subheading: "bold #d1d1d1",
    }
