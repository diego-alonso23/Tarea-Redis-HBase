"""Genera informe/informe.pdf a partir de informe/informe.html."""
from __future__ import annotations

from pathlib import Path

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from xhtml2pdf import pisa

RAIZ = Path(__file__).resolve().parent


def main() -> None:
    for nombre, archivo in (("Arial", "arial.ttf"), ("Arial-Bold", "arialbd.ttf")):
        ruta = Path("C:/Windows/Fonts") / archivo
        if ruta.exists():
            pdfmetrics.registerFont(TTFont(nombre, str(ruta)))

    html = (RAIZ / "informe.html").read_text(encoding="utf-8")
    salida = RAIZ / "informe.pdf"
    with salida.open("wb") as f:
        resultado = pisa.CreatePDF(src=html, dest=f, encoding="utf-8")
    if resultado.err:
        raise SystemExit(f"Errores al generar el PDF: {resultado.err}")
    print(f"PDF generado: {salida} ({salida.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
