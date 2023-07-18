# python "Other Resources\html_to_png.js" "Other Resources\data-diagram.html"
import asyncio
import os
from pyppeteer import launch

async def html_to_png(input_file_path):
    # Get the directory path and base filename from the input file path
    directory_path = os.path.dirname(input_file_path)
    base_filename = os.path.splitext(os.path.basename(input_file_path))[0]

    # Specify the output file path for the generated PNG image
    output_image_path = os.path.join(directory_path, f'{base_filename}.png')

    # Launch a headless browser instance
    browser = await launch()

    # Create a new page
    page = await browser.newPage()

    # Set the viewport size
    await page.setViewport({'width': 1280, 'height': 720})

    # Load the HTML file
    await page.goto(f'file://{input_file_path}')

    # Wait for the page to fully render (you can adjust the delay as needed)
    await asyncio.sleep(2)

    # Capture a screenshot of the page as a PNG image
    await page.screenshot({'path': output_image_path})

    # Close the browser
    await browser.close()

    print(f'Screenshot saved to {output_image_path}!')

# Example usage
files = ['engineer', 'features']
for f in files:
    input_file_path = r"C:\Users\MatiasVizcaino\repos\6203-DataAnalyticsBusiness-Project\Other Resources\data-diagram\{}.html".format(f)
    asyncio.run(html_to_png(input_file_path))
