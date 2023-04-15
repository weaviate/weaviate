import os
import shutil

def clear_base64_images():
    base_folder = "/mnt/nas1/fashion/base64_images"
    if os.path.exists(base_folder):
        shutil.rmtree(base_folder)
    os.mkdir(base_folder)

def convert_image_to_base64():
    img_path = "/mnt/nas1/fashion/images/"
    for file_path in os.listdir(img_path):
        filename = file_path.split("/")[-1]
        os.system(f"cat {img_path}{file_path} | base64 > /mnt/nas1/fashion/base64_images/{filename}.b64")
tmp = input('are you sure you want to clear all the base64 images? [Y/n]')
if tmp == "" or tmp == "y" or tmp == "Y":
    clear_base64_images()
    convert_image_to_base64()
    print('images have been converted')
else:
    print('okay cool')