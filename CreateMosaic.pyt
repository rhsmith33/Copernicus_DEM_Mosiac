# -*- coding: utf-8 -*-

import arcpy
import os
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from itertools import product
from pathlib import Path

class Toolbox:
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Copernicus Mosaic"
        self.alias = "CopernicusMosaic"

        # List of tool classes associated with this toolbox
        self.tools = [DEMTool, MosiacTool, MosaicLayer]

class DEMTool:
    keys = []

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Download Copernicus DEM"
        self.description = "Downloads the Copernicus DEM TIFs from AWS into project folder"

    def getParameterInfo(self):
        """Define the tool parameters."""

        #Create North, South, East, and West bounds for DEM data
        bound_north = arcpy.Parameter(
            displayName="North Bound (-90 - 90)",
            name="bound_north",
            datatype="GPLong",
            parameterType="Required",
            direction="Input")
        
        bound_north.filter.type = "Range"
        bound_north.filter.list = [-90, 90]

        bound_south = arcpy.Parameter(
            displayName="South Bound (-90 - 90)",
            name="bound_south",
            datatype="GPLong",
            parameterType="Required",
            direction="Input")
        
        bound_south.filter.type = "Range"
        bound_south.filter.list = [-90, 90]

        bound_east = arcpy.Parameter(
            displayName="East Bound (-180 - 180)",
            name="bound_east",
            datatype="GPLong",
            parameterType="Required",
            direction="Input")
        
        bound_east.filter.type = "Range"
        bound_east.filter.list = [-180, 180]

        bound_west = arcpy.Parameter(
            displayName="West Bound (-180 - 180)",
            name="bound_west",
            datatype="GPLong",
            parameterType="Required",
            direction="Input")
        
        bound_west.filter.type = "Range"
        bound_west.filter.list = [-180, 180]

        folder_path = arcpy.Parameter(
            displayName="Folder Path",
            name="folder_path",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")
        
        folder_name = arcpy.Parameter(
            displayName="New Folder Name",
            name="folder_name",
            datatype="GPString",
            parameterType="Optional",
            direction="Input")

        params = [bound_north, bound_south, bound_east, bound_west, folder_path, folder_name]
        return params

    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return
    
    #Creates a new folder to house AWS downloads
    def create_folder(parameters):
        arcpy.management.CreateFolder(parameters[4].valueAsText, parameters[5].valueAsText)
    
    #Creates the keys from the NSEW coordinates to make keys for AWS to understand
    def create_keys(parameters):
        
        northings = []
        eastings = []
        northings_list = []
        eastings_list = []

        if (abs(parameters[0].value) >= abs(parameters[1].value) and parameters[1].value > 0):
            for i in range(parameters[0].value - (parameters[1].value - 1)):
                northings.append(parameters[0].value - i)
                
        elif (abs(parameters[0].value) < abs(parameters[1].value) and parameters[0].value > 0):
            for i in range(abs(parameters[1].value - (parameters[0].value + 1))):
                northings.append(parameters[1].value - i)

        elif (abs(parameters[0].value) < abs(parameters[1].value) and parameters[0].value < 0):
            for i in range(abs(parameters[1].value - (parameters[0].value + 1))):
                northings.append(parameters[0].value - i)

        if (abs(parameters[2].value) >= abs(parameters[3].value) and parameters[3].value > 0):
            for i in range(parameters[2].value - (parameters[3].value - 1)):
                eastings.append(parameters[2].value - i)
                
        elif (abs(parameters[2].value) < abs(parameters[3].value) and parameters[2].value > 0):
            for i in range(abs(parameters[3].value - (parameters[2].value + 1))):
                eastings.append(parameters[2].value - i)

        elif (abs(parameters[2].value) < abs(parameters[3].value) and parameters[2].value < 0):
            for i in range(abs(parameters[3].value - (parameters[2].value + 1))):
                eastings.append(parameters[2].value - i)

        for i in northings:
            if (i >= 10 and parameters[0].value >= parameters[1].value):
                northings_list.append(f"_N{i}_00_")
            elif (i >= 10):
                northings_list.append(f"_S{i}_00_")
            elif (parameters[0].value >= parameters[1].value):
                northings_list.append(f"_N0{i}_00_")
            else:
                northings_list.append(f"_S0{i}_00_")

        for i in eastings:
            if (i >= 100 and parameters[2].value >= parameters[3].value):
                eastings_list.append(f"E{i}_00_")
            elif (i >= 100):
                eastings_list.append(f"W{abs(i)}_00_") 
            elif (i >= 10 and parameters[2].value >= parameters[3].value):
                eastings_list.append(f"E0{abs(i)}_00_")
            elif (i >= 10):
                eastings_list.append(f"W0{abs(i)}_00_")
            elif (parameters[2].value >= parameters[3].value):
                eastings_list.append(f"E00{i}_00_")
            else:
                eastings_list.append(f"W00{abs(i)}_00_")       
        #Create all combinations of northings and eastings
        tiles = [f"{x}{y}" for x in northings_list for y in eastings_list]
           
        for i in tiles:
            DEMTool.keys.append(f"Copernicus_DSM_COG_10{i}DEM/")

    #Downloads all files and folders matching the AWS keys made
    def get_file_folders(s3_client, bucket_name, prefix=""):
        file_names = []
        folders = []

        default_kwargs = {
            "Bucket": bucket_name,
            "Prefix": prefix
        }
        next_token = ""

        while next_token is not None:
            updated_kwargs = default_kwargs.copy()
            if next_token != "":
                updated_kwargs["ContinuationToken"] = next_token

            response = s3_client.list_objects_v2(**default_kwargs)
            contents = response.get("Contents")
            if contents is not None:
                for result in contents:
                    key = result.get("Key")
                    if key[-1] == "/":
                        folders.append(key)
                    else:
                        file_names.append(key)

            next_token = response.get("NextContinuationToken")

        return file_names, folders

    #Downloads the files from the found objects according to the matching keys
    def download_files(s3_client, bucket_name, local_path, file_names, folders):

        local_path = Path(local_path)

        for folder in folders:
            folder_path = Path.joinpath(local_path, folder)
            folder_path.mkdir(parents=True, exist_ok=True)

        for file_name in file_names:
            file_path = Path.joinpath(local_path, file_name)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            s3_client.download_file(
                bucket_name,
                file_name,
                str(file_path)
            )
            arcpy.AddMessage("Downloaded " + file_name)

    #Finds and downloads Copernicus DEM tiles according to the input coordinates        
    def execute(self, parameters, messages):
        """The source code of the tool."""

        #Bucket files are in format Copernicus_DSM_COG_10_[northing]_[easting]_DEM/
        s3 = boto3.client('s3', region_name='eu-central-1', config=Config(signature_version=UNSIGNED))

        folder_path = ""
        if (parameters[5].valueAsText) == None:
            folder_path = parameters[4].valueAsText
        else:
            DEMTool.create_folder(parameters)
            folder_path = parameters[4].valueAsText + r"\\" + parameters[5].valueAsText

        DEMTool.create_keys(parameters)
        for i in DEMTool.keys:
            file_names, folders = DEMTool.get_file_folders(s3, "copernicus-dem-30m", i)
            DEMTool.download_files(
            s3,
            'copernicus-dem-30m',
            folder_path,
            file_names,
            folders
        )
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
    
class MosiacTool:

    project = arcpy.mp.ArcGISProject("CURRENT")
    m = project.listMaps("Map")[0]

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create Copernicus Mosaic"
        self.description = "Adds Copernicus DEM TIFs to a single Mosaic Raster"

    def getParameterInfo(self):
        """Define the tool parameters."""
        mosaic_name = arcpy.Parameter(
            displayName="Mosaic Name",
            name="mosiac_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        
        spatial_reference = arcpy.Parameter(
            displayName="Spatial Reference",
            name="spatial_reference",
            datatype="GPCoordinateSystem",
            parameterType="Required",
            direction="Input")
        
        data_path = arcpy.Parameter(
            displayName="Data Path",
            name="data_path",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")
        
        gdb_path = arcpy.Parameter(
            displayName="Geodatabase Path",
            name="gdb_path",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input")
        
        mosaic = arcpy.Parameter(
            displayName="Mosaic",
            name="mosaic",
            datatype="DERasterDataset",
            parameterType="Derived",
            direction="Output")
    
        
        params = [mosaic_name, spatial_reference, data_path, gdb_path, mosaic]
        return params

    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return

    def create_Mosiac(parameters):
        arcpy.management.CreateMosaicDataset(arcpy.env.workspace, parameters[0].valueAsText, parameters[1].valueAsText)

    #Adds only the DEM tif files to the mosaic dataset
    def add_files(parameters) :
        for root, folders, files in os.walk(parameters[2].valueAsText) :
            if 'PREVIEW' in folders:
                folders.remove('PREVIEW')

            if 'INFO' in folders:
                folders.remove('INFO')

            if 'AUXFILES' in folders:
                folders.remove('AUXFILES')

            for file in files :
                if file.endswith('.xml'):
                    file_path = os.path.join(root, file)
                    os.remove(file_path)
                elif file.endswith('.kml'):
                    file_path = os.path.join(root, file)
                    os.remove(file_path)
                else:
                    filePath = r"{}".format(os.path.join(os.sep, root, file))
                    arcpy.AddMessage("Added " + filePath + " to Mosiac Raster")
                    arcpy.AddRastersToMosaicDataset_management(parameters[3].valueAsText + r"\\" + parameters[0].valueAsText, "Raster Dataset", filePath)
                    

    def execute(self, parameters, messages):
        MosiacTool.create_Mosiac(parameters)
        MosiacTool.add_files(parameters)
        MosiacTool.project.save()
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return

class MosaicLayer:

    project = arcpy.mp.ArcGISProject("CURRENT")
    m = project.listMaps("Map")[0]

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Add Mosiac Layer"
        self.description = "Adds Copernicus DEM Mosiac to Map"

    def getParameterInfo(self):
        """Define the tool parameters."""
        
        mosiac = arcpy.Parameter(
            displayName="Mosiac",
            name="mosiac",
            datatype="DERasterDataset",
            parameterType="Required",
            direction="Input")
        
        mosaic_name = arcpy.Parameter(
            displayName="Mosaic Name",
            name="mosiac_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input")
        
        gdb_path = arcpy.Parameter(
            displayName="Geodatabase Path",
            name="gdb_path",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input")
        
        lyr_path = arcpy.Parameter(
            displayName="Mosaic Path",
            name="mosaic_path",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")
        
        mosaic_lyr = arcpy.Parameter(
            displayName="Mosaic Layer",
            name="mosiac_lyr",
            datatype="GPMosaicLayer",
            parameterType="Derived",
            direction="Output")
        
        
        params = [mosiac, mosaic_name, gdb_path, lyr_path, mosaic_lyr]
        return params

    def isLicensed(self):
        """Set whether the tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        return
    
    #Creates a mosaic layer to preserve the dataset and adds it to current map
    def addMosaicLayer(parameters):
        mosaic_layer = arcpy.management.MakeMosaicLayer(parameters[0].value, parameters[1].value)
        arcpy.management.SaveToLayerFile(mosaic_layer, parameters[1].valueAsText)
        lyrFile = arcpy.mp.LayerFile(r"{}".format(parameters[3].valueAsText + "\\" + parameters[1].valueAsText + ".lyrx"))
        MosiacTool.m.addLayer(lyrFile)
        MosaicLayer.project.save()

    def execute(self, parameters, messages):
        arcpy.env.workspace = parameters[3].valueAsText
        MosaicLayer.addMosaicLayer(parameters)
        return

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
    