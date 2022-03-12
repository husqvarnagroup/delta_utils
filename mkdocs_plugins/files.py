from pathlib import Path

from mkdocs.config.config_options import Type  # type: ignore
from mkdocs.plugins import BasePlugin  # type: ignore
from mkdocs.structure.files import File  # type: ignore


class RootFiles(BasePlugin):
    config_scheme = (("files", Type(list, default=[])),)

    def on_files(self, files, config):
        project_path = Path(config["config_file_path"]).parent
        for filename in self.config["files"]:
            files.append(
                File(
                    filename,
                    project_path,
                    config["site_dir"],
                    config["use_directory_urls"],
                )
            )

        return files

    def on_serve(self, server, config, builder=None):
        project_path = Path(config["config_file_path"]).parent
        for filename in self.config["files"]:
            server.watch(str(project_path / filename), builder)
        return server

    def on_pre_page(self, page, config, files):
        # Change the edit urls for ROOT_FILES
        project_path = Path(config["config_file_path"]).parent
        for filename in self.config["files"]:
            root_file = project_path / filename
            if root_file.samefile(page.file.abs_src_path):
                page.edit_url = f"{config['repo_url']}edit/master/{filename}"
        return page
