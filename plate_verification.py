#!/usr/local/bin/python3

import tank
import argparse
import os
import logging
import sys
import re
import OpenImageIO as oiio
import timecode


class PlateVerification:

    # init takes two arguments - a SGTK engine, and a logger
    def __init__(self, engine, logger):
        self.engine = engine
        self.shotgun = engine.shotgun
        self.project = engine.context.project
        self.logger = logger
        self._shots = dict()
        self.bad_pfiles = dict()
        self.bad_versions = list()
        self._exclude_omits = False
        self.tag_plate = self.shotgun.find_one("Tag", [["name", "is", "Plate"]], ["name"])
        if not self.tag_plate:
            self.tag_plate = self.shotgun.create("Tag", {"name": "Plate"})
            self.tag_plate["name"] = "Plate"
        self.plate_pfile_types = {'shot_plate_frames': {'type': 'PublishedFileType', 'code': 'Plate EXR Sequence', 'id': 199},
                                  'shot_plate_avidmov': {'type': 'PublishedFileType', 'code': 'Plate Avid Movie', 'id': 200},
                                  'shot_plate_vfxmov': {'type': 'PublishedFileType', 'code': 'Plate VFX Movie', 'id': 201},
                                  'shot_plate_lut': {'type': 'PublishedFileType', 'code': 'Plate LUT', 'id': 202}}
        self.sg_sop_fields = ["sg_slope_red", "sg_slope_green", "sg_slope_blue", "sg_offset_red", "sg_offset_green",
                              "sg_offset_blue", "sg_power_red", "sg_power_green", "sg_power_blue"]
        self.filename_re = re.compile(
            r'^(?P<filehead>[0-9a-zA-Z_-]+)((\.(?P<frame>[0-9]{4,}))|)\.(?P<ext>[a-zA-Z0-9]{1,4})')
        self.plate_name_template = self.engine.get_template_by_name("plate_version_name")
        self.plate_pfile_templates = [self.engine.get_template_by_name("shot_plate_frames"),
                                      self.engine.get_template_by_name("shot_plate_avidmov"),
                                      self.engine.get_template_by_name("shot_plate_vfxmov"),
                                      self.engine.get_template_by_name("shot_plate_lut")]

    @property
    def exclude_omits(self):
        return self._exclude_omits
    @exclude_omits.setter
    def exclude_omits(self, exclude_bool):
        self._exclude_omits = exclude_bool

    @property
    def shots(self):
        return self._shots

    def retrieve_shots(self):
        self.logger.info("Retrieving complete list of shots from ShotGrid.")
        shot_template = self.engine.get_template_by_name("shot_root")
        sg_shot_filters = [['project', 'is', self.project], ['sg_shot_type', 'is_not', 'Bidding']]
        if self._exclude_omits:
            sg_shot_filters.append(['sg_status_list', 'is_not', 'omt'])
        sg_shots = self.shotgun.find("Shot",
                                     sg_shot_filters,
                                     ['code', 'sg_sequence'], order=[{'field_name': 'code', 'direction': 'asc'}])
        for sg_shot in sg_shots:
            shot_fs_path = shot_template.apply_fields({'Sequence': sg_shot['sg_sequence']['name'],
                                                       'Shot': sg_shot['code']})
            self.logger.debug("Adding Shot %s with filesystem path %s" % (sg_shot['code'], shot_fs_path))
            self._shots[sg_shot['code']] = {'dbobject': sg_shot, 'path': shot_fs_path}
        self.logger.info("Retrieved %d Shots from ShotGrid." % len(self._shots.keys()))

    def db_plates_for_shot(self, shot_name):
        shot_info = self._shots.get(shot_name)
        if not shot_info:
            logger.warning("Information for Shot %s is not available in memory!" % shot_name)
            return
        sg_plates = self.shotgun.find("Version",
                                      [["entity", "is", shot_info["dbobject"]], ["tags", "name_contains", "Plate"]],
                                      ['code', 'sg_first_frame', 'sg_first_frame_timecode', 'frame_count',
                                       'frame_range', 'sg_uploaded_movie_frame_rate', 'sg_last_frame', 'sg_clip_name',
                                       'sg_last_frame_timecode', 'sg_camera_roll', 'sg_lab_roll', 'sg_slope_red',
                                       'sg_slope_green', 'sg_slope_blue', 'sg_offset_red', 'sg_offset_green',
                                       'sg_offset_blue', 'sg_power_red', 'sg_power_green', 'sg_power_blue',
                                       'sg_saturation', 'sg_status_list', 'sg_uploaded_movie'])
        if len(sg_plates) == 0:
            logger.warning("Shot %s has no Plates!" % shot_name)
            return
        if not shot_info.get('plates'):
            shot_info['plates'] = dict()
        for sg_plate in sg_plates:
            self.logger.debug("For Shot %s: Located plate %s" % (shot_name, sg_plate["code"]))
            fields = self.plate_name_template.validate_and_get_fields(sg_plate["code"])
            if not fields:
                invalid_name_error_msg = "Plate in database %s has name that will not validate! Skipping." \
                                         % sg_plate["code"]
                self.logger.error(invalid_name_error_msg)
                bad_db_plate = {"dbobject": sg_plate,
                                "error_message": invalid_name_error_msg,
                                "name": sg_plate["code"]}
                self.bad_versions.append(bad_db_plate)
                continue
            if not shot_info['plates'].get(sg_plate["code"]):
                shot_info['plates'][sg_plate["code"]] = dict()
                shot_info['plates'][sg_plate["code"]]["dbobjects"] = list()
                shot_info['plates'][sg_plate["code"]]["int_version"] = 0
            if len(shot_info['plates'][sg_plate["code"]]["dbobjects"]) > 0:
                original_plate_name = shot_info['plates'][sg_plate["code"]]["dbobjects"][0]["code"]
                original_plate_id = shot_info['plates'][sg_plate["code"]]["dbobjects"][0]["id"]
                duplicate_plate_error = "In Shot %s, Plate %s with database ID %d is a duplicate of Plate %s with " \
                                        "database ID of %d." % (shot_name, sg_plate["code"], sg_plate["id"],
                                                                original_plate_name, original_plate_id)
                self.logger.error(duplicate_plate_error)
                bad_db_plate = {"dbobject": sg_plate,
                                "error_message": duplicate_plate_error,
                                "name": sg_plate["code"]}
                self.bad_versions.append(bad_db_plate)
                continue
            shot_info['plates'][sg_plate["code"]]["dbobjects"].append(sg_plate)
            shot_info['plates'][sg_plate["code"]]["int_version"] = fields["version"]
            if sg_plate["sg_status_list"] == 'cfrm':
                self.logger.info("For Shot %s, Plate %s has a database status of confirmed. Will skip Filesystem "
                                 "checks." % (shot_name, sg_plate["code"]))
                shot_info['plates'][sg_plate["code"]]["verified"] = True

    def filesystem_plates_for_shot(self, shot_name):
        shot_info = self._shots.get(shot_name)
        shot_plates_path = os.path.join(shot_info['path'], "plates")
        if not os.path.exists(shot_plates_path):
            shot_info["error_message"] = "Shot plates directory %s does not exist on the filesystem!" % shot_plates_path
            self.logger.error(shot_info["error_message"])
            return
        shot_all_plates_confirmed = True
        if shot_info.get('plates'):
            for plate_name, plate_object in shot_info['plates'].items():
                if not plate_object.get("verified"):
                    shot_all_plates_confirmed = False
                    break
            if shot_all_plates_confirmed:
                self.logger.info("For Shot %s, all Plates have a status of confirmed. "
                                 "Will skip filesystem checks." % shot_name)
                return
        self.logger.debug("Walking path %s" % shot_plates_path)
        found_files = dict()
        for cur_path, directories, files in os.walk(shot_plates_path):
            for file in files:
                is_seq = False
                filename_match = self.filename_re.match(file)
                if not filename_match:
                    self.logger.warning("Skipping file with bad name: %s" %
                                        os.path.join(shot_plates_path, cur_path, file))
                    continue
                # are we a sequence?
                match_dict = filename_match.groupdict()
                filename_list = file.split('.')
                pfile_name = file
                if match_dict.get("frame"):
                    pfile_name = '.'.join([filename_list[0],
                                           '%%0%dd' % len(match_dict["frame"]),
                                           filename_list[-1]])
                    is_seq = True
                if not found_files.get(pfile_name):
                    found_files[pfile_name] = dict()
                    found_files[pfile_name]["full_path"] = os.path.join(shot_plates_path, cur_path, pfile_name)
                    found_files[pfile_name]["is_seq"] = is_seq
                    found_files[pfile_name]["name"] = pfile_name
                    found_files[pfile_name]["size"] = 0
                    if is_seq:
                        found_files[pfile_name]["frames"] = list()
                if is_seq:
                    found_files[pfile_name]["frames"].append(file)
                full_file_path = os.path.join(shot_plates_path, cur_path, file)
                found_files[pfile_name]["size"] += os.path.getsize(full_file_path)
        if len(found_files.keys()) == 0:
            no_plates_error_message = "In Shot %s, Plate directory exists at %s, but it does not contain anything " \
                                      "that can be classified as a Plate!" % (shot_name, shot_plates_path)
            self.logger.error(no_plates_error_message)
            shot_info["error_message"] = no_plates_error_message
            return
        self.logger.debug("Examining collected files...")
        for pfile_name in found_files.keys():
            is_pfile_valid = False
            this_version_name = None
            fields = None
            for template in self.plate_pfile_templates:
                fields = template.validate_and_get_fields(found_files[pfile_name]["full_path"])
                if fields:
                    is_pfile_valid = True
                    found_files[pfile_name]['match_template'] = template.name
                    found_files[pfile_name]['published_file_type'] = self.plate_pfile_types[template.name]
                    this_version_name = self.plate_name_template.apply_fields(fields)
                    break
            if not is_pfile_valid:
                self.logger.error("Tossing out file %s - does not match naming convention."
                                  % found_files[pfile_name]["full_path"])
                found_files[pfile_name]["error_message"] = "File %s is likely in wrong subfolder - does not match any" \
                                                           " naming convention." % found_files[pfile_name]["full_path"]
                self.bad_pfiles[pfile_name] = found_files[pfile_name]
            else:
                version_metadata = None
                if found_files[pfile_name]["is_seq"]:
                    self.logger.debug("Located image sequence %s - extracting metadata." % pfile_name)
                    version_metadata = dict()
                    frames_sorted = sorted(found_files[pfile_name]["frames"])
                    first_frame_base = frames_sorted[0]
                    last_frame_base = frames_sorted[-1]
                    first_frame_number = int(first_frame_base.split('.')[1])
                    version_metadata["sg_first_frame"] = first_frame_number
                    last_frame_number = int(last_frame_base.split('.')[1])
                    version_metadata["sg_last_frame"] = last_frame_number
                    version_metadata["frame_count"] = last_frame_number - first_frame_number + 1
                    version_metadata["frame_range"] = "%s-%s" % (first_frame_number, last_frame_number)
                    imgseq_directory = os.path.dirname(found_files[pfile_name]["full_path"])
                    first_frame_path = os.path.join(imgseq_directory, first_frame_base)
                    last_frame_path = os.path.join(imgseq_directory, last_frame_base)
                    firstin = oiio.ImageInput.open(first_frame_path)
                    if not firstin:
                        exr_parse_err = "Unable to open first frame for EXR sequence at %s!" % first_frame_path
                        found_files[pfile_name]["error_message"] = exr_parse_err
                        self.bad_pfiles[pfile_name] = found_files[pfile_name]
                        self.logger.error(exr_parse_err)
                        continue
                    firstspec = firstin.spec()
                    first_framerate_numerator = firstspec.getattribute("framerate_numerator")
                    first_framerate_denominator = firstspec.getattribute("framerate_denominator")
                    this_framerate = 24.0
                    if first_framerate_denominator and first_framerate_numerator:
                        this_framerate = float(first_framerate_numerator)/float(first_framerate_denominator)
                    first_tc_string = firstspec.getattribute("frame_absolute_timecode")
                    version_metadata["sg_first_frame_timecode"] = 0
                    if first_tc_string:
                        first_frame_tc = timecode.Timecode(this_framerate, start_timecode=first_tc_string)
                        version_metadata["sg_first_frame_timecode"] = int(first_frame_tc.frames/this_framerate*1000.0)
                    version_metadata["sg_lab_roll"] = firstspec.getattribute("reel_id_full")
                    if firstspec.getattribute("reel_id_full"):
                        version_metadata["sg_camera_roll"] = firstspec.getattribute("reel_id_full").split("_")[0]
                    # All the CDL crap
                    version_metadata["sg_saturation"] = 1.0
                    if firstspec.getattribute("mpl.asc_sat"):
                        version_metadata["sg_saturation"] = float(firstspec.getattribute("mpl.asc_sat"))
                    asc_sop_array = [1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0]
                    asc_sop_text = firstspec.getattribute("mpl.asc_sop")
                    if asc_sop_text:
                        asc_sop_cleanup = asc_sop_text.replace(')(', ' ').replace('(', '').replace(')', '')
                        for sop_idx, sop_val in enumerate(asc_sop_cleanup.split(" ")):
                            if sop_idx >= len(asc_sop_array):
                                break
                            asc_sop_array[sop_idx] = float(sop_val)
                    for sop_idx, sop_field_name in enumerate(self.sg_sop_fields):
                        version_metadata[sop_field_name] = asc_sop_array[sop_idx]
                    firstin.close()
                    lastin = oiio.ImageInput.open(last_frame_path)
                    if not lastin:
                        exr_parse_err = "Unable to open first frame for EXR sequence at %s!" % last_frame_path
                        found_files[pfile_name]["error_message"] = exr_parse_err
                        self.bad_pfiles[pfile_name] = found_files[pfile_name]
                        self.logger.error(exr_parse_err)
                        continue
                    lastspec = lastin.spec()
                    last_tc_string = lastspec.getattribute("frame_absolute_timecode")
                    version_metadata["sg_last_frame_timecode"] = 0
                    if last_tc_string:
                        last_frame_tc = timecode.Timecode(this_framerate, start_timecode=last_tc_string)
                        version_metadata["sg_last_frame_timecode"] = int(last_frame_tc.frames/this_framerate*1000.0)
                    lastin.close()
                    self.logger.debug("Extracted version metadata: %s" % version_metadata)
                    self.logger.debug("Checking directory %s to make sure there are no missing frames..."
                                      % imgseq_directory)
                    avg_frame_size = found_files[pfile_name]["size"]/(last_frame_number - first_frame_number + 1)
                    for frame_number in range(first_frame_number, last_frame_number + 1):
                        full_frame_path = os.path.join(imgseq_directory, pfile_name % frame_number)
                        if not os.path.exists(full_frame_path):
                            frame_missing_err = "Plate %s missing frame %d at path %s!" \
                                                % (pfile_name, frame_number, full_frame_path)
                            if found_files[pfile_name].get("error_message"):
                                found_files[pfile_name]["error_message"] += "\n" + frame_missing_err
                            self.logger.error(frame_missing_err)
                            self.bad_pfiles[pfile_name] = found_files[pfile_name]
                            continue
                        frame_size = os.path.getsize(full_frame_path)
                        if frame_size > (1.25*avg_frame_size) or frame_size < (0.75*avg_frame_size):
                            frame_size_err = "Plate %s has frame %d with deviant file size of %d bytes at path %s." % (pfile_name, frame_number, frame_size, full_frame_path)
                            if found_files[pfile_name].get("error_message"):
                                found_files[pfile_name]["error_message"] += "\n" + frame_size_err
                            self.logger.error(frame_size_err)
                            self.bad_pfiles[pfile_name] = found_files[pfile_name]

                if not shot_info.get('plates'):
                    self.logger.warning("Shot %s has no plates in the database, and yet, they are here on the "
                                        "filesystem!" % shot_name)
                    shot_info["plates"] = dict()
                if not shot_info['plates'].get(this_version_name):
                    self.logger.warning("Unable to find plate in database %s in shot %s." %
                                        (this_version_name, shot_name))
                    shot_info['plates'][this_version_name] = dict()
                if not shot_info['plates'][this_version_name].get("published_files"):
                    shot_info['plates'][this_version_name]["published_files"] = list()
                if version_metadata:
                    shot_info['plates'][this_version_name]["version_metadata"] = version_metadata
                shot_info['plates'][this_version_name]["published_files"].append(found_files[pfile_name])
                self.logger.debug("Found: Shot %s, Version %s, PublishedFile %s, match template %s, full path %s" %
                                  (shot_name, this_version_name, pfile_name, found_files[pfile_name]['match_template'],
                                   found_files[pfile_name]['full_path']))

    def reconcile_db_with_filesystem(self, shot_name):
        shot_info = self._shots.get(shot_name)
        plates_list = shot_info.get('plates')
        if not plates_list:
            self.logger.error("Shot %s has no plates, either in the database or on the filesystem!" % shot_name)
            return
        for plate_name, plate_object in plates_list.items():
            new_db_version = False
            if plate_object.get("verified"):
                self.logger.info("For Shot %s, Plate %s has already been verified. Will skip reconciliation."
                                 % (shot_name, plate_name))
                continue
            this_sg_plate = None
            if not plate_object.get("dbobjects"):
                plate_object["dbobjects"] = list()
                new_db_version = True
            else:
                if len(plate_object["dbobjects"]) == 0:
                    new_db_version = True
                else:
                    this_sg_plate = plate_object["dbobjects"][0]
            version_metadata = plate_object.get("version_metadata")
            if not version_metadata:
                no_vmd_error_message = "For Shot %s, Plate %s has no version metadata attribute. This should only " \
                                       "happen if all plates for a Shot have a status of confirmed." \
                                       % (shot_name, plate_name)
                self.logger.error(no_vmd_error_message)
                shot_info["error_message"] = no_vmd_error_message
                continue
            filesystem_pfiles = plate_object.get("published_files")
            if not filesystem_pfiles:
                no_files_error_message = "For Shot %s, Plate %s has no verifiable files that exist on the Filesystem." \
                                         % (shot_name, plate_name)
                self.logger.error(no_files_error_message)
                if not shot_info.get("error_message"):
                    shot_info["error_message"] = no_files_error_message
                continue
            sg_pfiles_for_plate = list()
            version_update_data = {'sg_status_list': 'cfrm'}
            if new_db_version:
                self.logger.info("Will create new Plate in database %s for Shot %s." % (plate_name, shot_name))
                version_metadata["tags"] = [self.tag_plate]
                version_metadata["project"] = self.project
                version_metadata["entity"] = shot_info["dbobject"]
                version_metadata["sg_link___shot"] = shot_info["dbobject"]
                version_metadata["sg_status_list"] = "na"
                version_metadata["code"] = plate_name
                this_sg_plate = self.shotgun.create("Version", version_metadata)
                this_sg_plate.update(version_metadata)
                plate_object["dbobjects"].append(this_sg_plate)
            else:
                # make sure that filesystem frame count and timecode matches DB
                fields_to_match = ['frame_count', 'sg_first_frame_timecode', 'sg_last_frame_timecode']
                validate_fields_err_msg = list()
                for field in fields_to_match:
                    sgp_field_value = None
                    vmd_field_value = None
                    if this_sg_plate.get(field):
                        sgp_field_value = this_sg_plate[field]
                    if version_metadata.get(field):
                        vmd_field_value = version_metadata[field]
                    if sgp_field_value and vmd_field_value:
                        if field.endswith("timecode"):
                            sgp_field_value = timecode.Timecode(24.0, start_seconds=float(sgp_field_value)/1000.0)
                            vmd_field_value = timecode.Timecode(24.0, start_seconds=float(vmd_field_value) / 1000.0)
                        if sgp_field_value != vmd_field_value:
                            err_msg_string = "%s in database value of %s does not match %s in " \
                                             "filesystem value of %s!" \
                                             % (field, sgp_field_value, field, vmd_field_value)
                            validate_fields_err_msg.append(err_msg_string)
                if len(validate_fields_err_msg) > 0:
                    complete_err_msg = '\n'.join(validate_fields_err_msg)
                    bad_db_plate = {"dbobject": this_sg_plate,
                                    "error_message": complete_err_msg,
                                    "name": plate_name}
                    self.bad_versions.append(bad_db_plate)
                self.logger.debug("Updating plate %s with frame range information from the filesystem." % plate_name)
                version_update_data["frame_count"] = version_metadata["frame_count"]
                version_update_data["sg_last_frame_timecode"] = version_metadata["sg_last_frame_timecode"]
                version_update_data["sg_first_frame_timecode"] = version_metadata["sg_first_frame_timecode"]
                version_update_data["sg_first_frame"] = version_metadata["sg_first_frame"]
                version_update_data["sg_last_frame"] = version_metadata["sg_last_frame"]
                version_update_data["frame_range"] = version_metadata["frame_range"]
                self.logger.debug("Looking through the database for anything already published under Version %s..."
                                  % plate_name)
                sg_pfiles_for_plate = self.shotgun.find("PublishedFile", [["version", "is", this_sg_plate]], ["code"])
                for sg_pfile in sg_pfiles_for_plate:
                    for fs_pfile in filesystem_pfiles:
                        if sg_pfile["code"] == fs_pfile["name"]:
                            fs_pfile["already_published"] = True
                            self.logger.info("PublishedFile %s already exists in the database with ID %d. Will Skip."
                                             % (sg_pfile["code"], sg_pfile["id"]))
                            break
            shot_context = self.engine.sgtk.context_from_entity_dictionary(shot_info["dbobject"])
            for fs_pfile in filesystem_pfiles:
                if fs_pfile["match_template"] == "shot_plate_frames":
                    version_update_data["sg_path_to_frames"] = fs_pfile["full_path"]
                elif fs_pfile["match_template"] == "shot_plate_avidmov":
                    version_update_data["sg_path_to_movie"] = fs_pfile["full_path"]
                elif fs_pfile["match_template"] == "shot_plate_vfxmov":
                    version_update_data["sg_path_to_vfx_movie"] = fs_pfile["full_path"]
                elif fs_pfile["match_template"] == "shot_plate_lut":
                    version_update_data["sg_path_to_lut"] = fs_pfile["full_path"]
                if fs_pfile.get("already_published"):
                    continue
                self.logger.info("Publishing %s." % fs_pfile["name"])
                po_int_version = plate_object.get("int_version")
                if not po_int_version:
                    self.logger.error("Plate object %s has no integer version number! Defaulting to 1." % plate_name)
                    po_int_version = 1
                publish_loop = True
                while publish_loop:
                    try:
                        sg_pfile = sgtk.util.register_publish(self.engine.sgtk, shot_context, fs_pfile["full_path"],
                                                              fs_pfile["name"], po_int_version,
                                                              published_file_type=fs_pfile["published_file_type"]["code"],
                                                              version_entity=this_sg_plate)
                        # Handle stupid ConnectionResetErrors, just keep looping until it goes through
                        publish_loop = False
                        break
                    except ConnectionResetError as crerr:
                        self.logger.warning("Got ConnectionResetError while attempting to publish. Will keep trying.")
                self.logger.debug("Successfully published %s with database ID %d." % (fs_pfile["name"], sg_pfile["id"]))
                fs_pfile["already_published"] = True
            if version_update_data.get("sg_path_to_movie"):
                if this_sg_plate.get("sg_uploaded_movie"):
                    self.logger.debug("Skipping upload of movie for Plate %s - movie data already exists." % plate_name)
                else:
                    upload_file = True
                    movie_path = version_update_data["sg_path_to_movie"]
                    if not os.path.exists(movie_path):
                        self.logger.error("Movie file %s does not exist!" % movie_path)
                        movie_path = version_update_data.get("sg_path_to_vfx_movie")
                        if not movie_path:
                            self.logger.error("VFX Movie file %s does not exist either!" % movie_path)
                            upload_file = False
                        else:
                            if os.path.getsize(movie_path) == 0:
                                self.logger.error("VFX Movie file %s is empty!" % movie_path)
                                upload_file = False
                    else:
                        if os.path.getsize(movie_path) == 0:
                            self.logger.error("Movie file %s is empty!" % movie_path)
                            movie_path = version_update_data.get("sg_path_to_vfx_movie")
                            if not movie_path:
                                self.logger.error("VFX Movie file %s does not exist either!" % movie_path)
                                upload_file = False
                            else:
                                if os.path.getsize(movie_path) == 0:
                                    self.logger.error("VFX Movie file %s is empty!" % movie_path)
                                    upload_file = False
                    if upload_file:
                        self.logger.info("For Plate %s: uploading movie %s..."
                                         % (plate_name, movie_path))
                        self.shotgun.upload("Version",
                                            this_sg_plate["id"],
                                            movie_path,
                                            field_name="sg_uploaded_movie")
                    else:
                        self.logger.error("Unable to upload movie for plate %s." % plate_name)
            # set version status to confirmed, since we've done all the work, and update plate paths
            self.logger.debug("Updating Version %s with update data %s" % (plate_name, version_update_data))
            self.shotgun.update("Version", this_sg_plate["id"], version_update_data)

    def print_all_errors(self):
        self.logger.info("Errors pertaining to Shots in the database:")
        found_shot_err = False
        for shot_name, shot_info in self._shots.items():
            if shot_info.get("error_message"):
                self.logger.error("Shot %s: %s" % (shot_name, shot_info.get("error_message")))
                found_shot_err = True
        if not found_shot_err:
            self.logger.info("No Shot errors found!")
        self.logger.info("Errors pertaining to Versions/Plates in the database:")
        found_version_error = False
        for plate_info in self.bad_versions:
            found_version_error = True
            plate_name = plate_info["name"]
            self.logger.error("Plate %s: %s" % (plate_name, plate_info.get("error_message")))
        if not found_version_error:
            self.logger.info("No Version/Plate errors found!")
        self.logger.info("Errors pertaining to files on the filesystem:")
        found_pfile_error = False
        for pfile_name, pfile_info in self.bad_pfiles.items():
            found_pfile_error = True
            self.logger.error("File %s: %s" % (pfile_name, pfile_info.get("error_message")))
        if not found_pfile_error:
            self.logger.info("No file errors found!")


if __name__ == "__main__":
    this_script = os.path.basename(__file__)
    argparser = argparse.ArgumentParser(prog=this_script)
    argparser.add_argument('-d', '--debug', help='Prints debugging output on the console.', action='store_true')
    argparser.add_argument('-o', '--exclude_omits', help='Excludes omitted shots from processing', action='store_true')
    argparser.add_argument('-s', '--sg_site', help='Specify the name of the ShotGrid site you with to connect to, '
                                                   'e.g. \"tracevfx\".', default='tracevfx')
    argparser.add_argument('-p', '--sg_project', help='Specify the name of the project you wish to work with.',
                           default='TRACE')
    argparser.add_argument('-c', '--pipeline_config', help='Specify a specific Pipeline Configuration to use.',
                           default="Primary")
    argparser.add_argument('-l', '--record-limit', type=int, help='Limit processing to X number of Shots', default=-1)
    pgm_args = argparser.parse_args()
    logger = logging.getLogger(this_script)
    st_handler = logging.StreamHandler()
    lfmt = logging.Formatter('[%(name)s] : [%(levelname)s] : %(message)s')
    st_handler.setFormatter(lfmt)
    logger.addHandler(st_handler)
    if pgm_args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    record_limit = 9999
    if pgm_args.record_limit > 0:
        logger.info("Limiting processing to %d shots." % pgm_args.record_limit)
        record_limit = pgm_args.record_limit
    if pgm_args.exclude_omits:
        logger.info("Will not include omitted Shots in processing.")
    # Current ShotGrid host
    sg_host = "https://%s.shotgunstudio.com" % pgm_args.sg_site
    project_name = pgm_args.sg_project
    pc_name = pgm_args.pipeline_config
    engine_name = "tk-shell"
    logger.info("Setting ShotGrid host to %s." % sg_host)
    # Authenticate with ShotGrid
    sg_dm = tank.authentication.DefaultsManager(fixed_host=sg_host)
    sg_sa = tank.authentication.ShotgunAuthenticator(defaults_manager=sg_dm)
    sg_existing_user = sg_dm.get_user_credentials()
    if not sg_existing_user:
        logger.warning("No existing user credentials exist. Will prompt.")
    sg_user = sg_sa.get_user()
    sg_existing_user = sg_dm.get_user_credentials()
    current_username = ""
    current_usertype = ""
    if sg_existing_user.get('login'):
        current_username = sg_existing_user['login']
        current_usertype = "User"
    else:
        current_username = sg_existing_user['api_script']
        current_usertype = "Script API User"
    sg = sg_user.create_sg_connection()
    logger.info("Connection to ShotGrid initialized with %s %s." % (current_usertype, current_username))
    # find the project entity ID
    sg_project = sg.find_one("Project", [['name', 'is', project_name]])
    if not sg_project:
        logger.critical("Project %s does not exist in ShotGrid site %s." % (project_name, sg_host))
        sys.exit(-1)
    logger.info("Located project %s in ShotGrid with ID %d." % (project_name, sg_project['id']))
    sg_tkmgr = tank.bootstrap.ToolkitManager(sg_user=sg_user)
    sg_tkmgr.plugin_id = "basic.*"
    sg_tkmgr.base_configuration = "sgtk:descriptor:app_store?name=tk-config-basic"
    sg_pc_list = sg_tkmgr.get_pipeline_configurations(sg_project)
    selected_pc = None
    for sg_pc in sg_pc_list:
        if sg_pc['name'] == pc_name:
            logger.info("Located PipelineConfiguration %s for Project %s." % (pc_name, project_name))
            selected_pc = sg_pc
    if not selected_pc:
        logger.critical("Unable to locate a PipelineConfiguration named %s in Project %s!" % (pc_name, project_name))
        sys.exit(-1)
    sg_tkmgr.pipeline_configuration = pc_name
    # make sure that the pipeline config has been localized as well as the core.
    selected_pc_descriptor = selected_pc['descriptor']
    logger.info("Got ConfigDescriptor %s for PipelineConfiguration %s." %
                (selected_pc_descriptor.display_name, pc_name))
    selected_pc_descriptor.ensure_local()
    logger.info("PipelineConfiguration %s cached locally at %s." % (pc_name, selected_pc_descriptor.get_path()))
    core_descriptor = tank.descriptor.create_descriptor(sg, tank.descriptor.Descriptor.CORE,
                                                        selected_pc_descriptor.associated_core_descriptor)
    logger.info("Got CoreDescriptor %s for PipelineConfiguration %s." % (core_descriptor.display_name, pc_name))
    core_descriptor.ensure_local()
    new_sgtk_path = os.path.join(core_descriptor.get_path(), "python")
    logger.info("tk-core cached locally at %s." % new_sgtk_path)
    # try to find the installed core location
    # installed_config_loc = sgtk.util.LocalFileStorageManager.get_configuration_root(sg_host, sg_project['id'], 'basic.*', selected_pc['id'], sgtk.util.LocalFileStorageManager.CACHE)
    logger.info("Will add to system PYTHONPATH.")
    # force delete the object and the entire sgtk library, since we will recreate it
    sg_loaded_modules = list()
    for mname in sys.modules.keys():
        if mname.startswith('tank') or mname.startswith('sgtk'):
            sg_loaded_modules.append(mname)
    del sg_tkmgr
    del sg
    del sg_user
    del sg_sa
    del sg_dm
    for mname in sg_loaded_modules:
        del sys.modules[mname]
    del tank
    sys.path.insert(0, new_sgtk_path)
    import sgtk

    logger.info("Reloaded SGTK from cached location.")
    logger.info("New SGTK module path: %s" % sgtk.get_sgtk_module_path())
    sg_dm_new = sgtk.authentication.DefaultsManager(fixed_host=sg_host)
    sg_sa_new = sgtk.authentication.ShotgunAuthenticator(defaults_manager=sg_dm_new)
    sg_user_new = sg_sa_new.get_user()
    # now that we have the proper cached SGTK code, redo the whole freaking thing
    sg_tkmgr_new = sgtk.bootstrap.ToolkitManager(sg_user=sg_user_new)
    sg_tkmgr_new.plugin_id = "basic.*"
    sg_tkmgr_new.base_configuration = "sgtk:descriptor:app_store?name=tk-config-basic"
    sg_tkmgr_new.pipeline_configuration = pc_name
    # sg_tkmgr_new.prepare_engine(engine_name, entity=sg_project)
    sg_engine = sg_tkmgr_new.bootstrap_engine(engine_name, entity=sg_project)
    logger.info("Successfully bootstrapped SGTK %s engine." % engine_name)
    pv = PlateVerification(sg_engine, logger)
    pv.exclude_omits = pgm_args.exclude_omits
    pv.retrieve_shots()
    for idx, shot in enumerate(pv.shots.keys()):
        pv.db_plates_for_shot(shot)
        if 0 < record_limit < (idx + 2):
            break
    for idx, shot in enumerate(pv.shots.keys()):
        pv.filesystem_plates_for_shot(shot)
        if 0 < record_limit < (idx + 2):
            break
    for idx, shot in enumerate(pv.shots.keys()):
        pv.reconcile_db_with_filesystem(shot)
        if 0 < record_limit < (idx + 2):
            break
    logger.info("List of all errors encountered:")
    pv.print_all_errors()
    logger.info("All done!")

