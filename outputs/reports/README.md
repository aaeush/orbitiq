# OrbitIQ LaTeX Report

This directory contains the LaTeX source files for the OrbitIQ Space Launch Data Analysis Report.

## Files

- `orbitiq_analysis_report.tex` - Main LaTeX document
- `README.md` - This file

## Installation Requirements

To compile the LaTeX document, you need to install LaTeX on your system:

### macOS
```bash
# Install Homebrew (if not already installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install MacTeX (full LaTeX distribution)
brew install --cask mactex

# Or install BasicTeX (minimal distribution)
brew install --cask basictex
```

### Ubuntu/Debian
```bash
sudo apt update
sudo apt install texlive-full
```

### Windows
Download and install MiKTeX from: https://miktex.org/download

## Compilation

Once LaTeX is installed, compile the document:

```bash
# Navigate to the reports directory
cd outputs/reports

# Compile the LaTeX document
pdflatex orbitiq_analysis_report.tex

# For bibliography and cross-references, run multiple times:
pdflatex orbitiq_analysis_report.tex
bibtex orbitiq_analysis_report
pdflatex orbitiq_analysis_report.tex
pdflatex orbitiq_analysis_report.tex
```

## Alternative: Online LaTeX Editors

If you prefer not to install LaTeX locally, you can use online editors:

1. **Overleaf** (https://www.overleaf.com) - Popular online LaTeX editor
2. **ShareLaTeX** (now part of Overleaf)
3. **Papeeria** (https://papeeria.com)

Simply upload the `.tex` file to any of these platforms.

## Document Features

The report includes:

- **Professional formatting** with custom colors (#DEFF9A accent)
- **Comprehensive analysis** of space launch data
- **Statistical insights** with proper LaTeX math formatting
- **Code listings** with Python syntax highlighting
- **Tables and figures** with proper captions
- **References** and appendices
- **Table of contents** with hyperlinks

## Customization

The document uses several custom elements:

- **Colors**: Defined in the preamble (`accentcolor`, `darkgray`, `lightgray`)
- **Headers/Footers**: Custom styling with project branding
- **Code blocks**: Python syntax highlighting
- **Figures**: Placeholder references to generated visualizations

## Troubleshooting

If you encounter compilation errors:

1. **Missing packages**: Install the full LaTeX distribution (texlive-full or MacTeX)
2. **Font issues**: Ensure T1 font encoding is available
3. **Figure errors**: Check that figure paths are correct
4. **Bibliography errors**: Run `bibtex` if using citations

## Output

The compiled document will be:
- `orbitiq_analysis_report.pdf` - The final report

This creates a professional, publication-ready analysis report of your OrbitIQ space launch data analysis.
