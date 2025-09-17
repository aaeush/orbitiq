#!/bin/bash

# OrbitIQ LaTeX Report Compilation Script
# This script compiles the LaTeX document with proper error handling

echo "🚀 OrbitIQ LaTeX Report Compilation"
echo "===================================="

# Check if LaTeX is installed
if ! command -v pdflatex &> /dev/null; then
    echo "❌ pdflatex not found!"
    echo ""
    echo "📦 Install LaTeX:"
    echo "  macOS:    brew install --cask mactex"
    echo "  Ubuntu:   sudo apt install texlive-full"
    echo "  Windows:  Download MiKTeX from https://miktex.org/"
    echo ""
    echo "🌐 Or use online: https://www.overleaf.com"
    exit 1
fi

# Set the document name
DOC_NAME="orbitiq_analysis_report"

echo "📄 Compiling: ${DOC_NAME}.tex"
echo ""

# First compilation
echo "🔄 First pass..."
pdflatex -interaction=nonstopmode ${DOC_NAME}.tex

if [ $? -eq 0 ]; then
    echo "✅ First pass completed successfully"
else
    echo "❌ First pass failed"
    exit 1
fi

# Second compilation (for cross-references)
echo "🔄 Second pass..."
pdflatex -interaction=nonstopmode ${DOC_NAME}.tex

if [ $? -eq 0 ]; then
    echo "✅ Second pass completed successfully"
else
    echo "❌ Second pass failed"
    exit 1
fi

# Clean up auxiliary files
echo "🧹 Cleaning up auxiliary files..."
rm -f ${DOC_NAME}.aux ${DOC_NAME}.log ${DOC_NAME}.toc ${DOC_NAME}.out

echo ""
echo "🎉 Compilation completed successfully!"
echo "📄 Output: ${DOC_NAME}.pdf"
echo ""

# Check if PDF was created
if [ -f "${DOC_NAME}.pdf" ]; then
    echo "📊 Document statistics:"
    echo "   File size: $(du -h ${DOC_NAME}.pdf | cut -f1)"
    echo "   Pages: $(pdfinfo ${DOC_NAME}.pdf 2>/dev/null | grep Pages | awk '{print $2}' || echo 'Unknown')"
    echo ""
    echo "🔍 Open with:"
    echo "   macOS:    open ${DOC_NAME}.pdf"
    echo "   Linux:    xdg-open ${DOC_NAME}.pdf"
    echo "   Windows:  start ${DOC_NAME}.pdf"
else
    echo "❌ PDF file was not created"
    exit 1
fi
