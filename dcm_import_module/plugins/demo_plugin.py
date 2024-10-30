"""
This module defines a demo plugin-class for faked import of IEs
"""

from typing import Optional
from datetime import datetime
from uuid import uuid4
import random

from dcm_common import LoggingContext as Context, Logger
from dcm_common.models.report import Status

from dcm_import_module.plugins import Interface
from dcm_import_module.models import JSONType, Signature, Argument, IE, \
    PluginResult


class DemoPlugin(Interface):
    _NAME = "demo"
    _DESCRIPTION = "Demo Plugin"
    _DEPENDENCIES = [
    ]
    _SIGNATURE = Signature(
        number=Argument(
            type_=JSONType.INTEGER,
            required=False,
            description="number of generated IEs",
            example=2,
            default=1
        ),
        randomize=Argument(
            type_=JSONType.BOOLEAN,
            required=False,
            description="randomize generated metadata",
            example=True,
            default=False
        ),
    )
    _TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>RESPONSE_DATETIME</responseDate>
  <request identifier="IDENTIFIER" metadataPrefix="oai_dc" verb="GetRecord">https://www.lzv.nrw/</request>
  <GetRecord>
    <record>
      <header>
        <identifier>IDENTIFIER</identifier>
        <datestamp>DATE</datestamp>
      </header>
      <metadata>
        <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
          <dc:title>TITLE</dc:title>
          <dc:language>en</dc:language>
          <dc:creator>CREATOR</dc:creator>
          <dc:contributor>INSTITUTION</dc:contributor>
          <dc:subject>SUBJECT</dc:subject>
          <dc:date>DATE</dc:date>
          <dc:type>article</dc:type>
          <dc:format>application/pdf</dc:format>
          <dc:identifier>https://www.lzv.nrw/some_file</dc:identifier>
          <dc:rights>CC BY-NC 4.0</dc:rights>
          <dc:rights>info:eu-repo/semantics/openAccess</dc:rights>
        </oai_dc:dc>
      </metadata>
    </record>
  </GetRecord>
</OAI-PMH>"""
    _FIRSTNAMES = ["Bartholomew", "Priscilla", "Nigel", "Percival", "Tabatha"]
    _SURNAMES = [
        "Featherstonehaugh", "Thistlethwaite", "Crumpetsworth", "Bottombean"
    ]
    _SUBJECTS = [
        "Quantum Entanglement", "Nanotechnology", "Number Theory",
        "Dynamical Systems", "Human Physiology", "Linguistics",
        "Political Science"
    ]
    _INSTITUTIONS = [
        "Harvard University",
        "Massachusetts Institute of Technology (MIT)",
        "University of Oxford",
        "Stanford University",
        "Princeton University",
        "Yale University"
    ]
    _TITLES = [
        "Dancing {} and the Quantum {}",
        "Whispers from the {}: Unveiling the secrets of {}",
        "Decoding the {}: When a {} changes the world",
        "Symphony of the {}: Mapping the Music of {}",
        "Unmasking the {}: Understanding the Enigmas of {}"
    ]
    _WORDS = [
        "Photons", "Butterfly Effect", "Time", "Brain", "Consciousness",
        "Fractals", "Chaos", "Law", "Humanity", "Genome", "Language"
    ]

    @classmethod
    def generate_metadata(
        cls, randomize: bool = False, identifier: Optional[str] = None
    ) -> str:
        creator = (
                random.choice(cls._SURNAMES)
                + ", "
                + random.choice(cls._FIRSTNAMES)
        ) if randomize else (
            cls._SURNAMES[0] + ", " + cls._FIRSTNAMES[0]
        )
        subject = random.choice(cls._SUBJECTS) \
            if randomize else cls._SUBJECTS[0]
        institution = random.choice(cls._INSTITUTIONS) \
            if randomize else cls._INSTITUTIONS[0]
        _title = random.choice(cls._TITLES) if randomize else cls._TITLES[0]
        title = _title.format(
            random.choice(cls._WORDS), random.choice(cls._WORDS)
        ) if randomize else _title.format(cls._WORDS[0], cls._WORDS[1])
        return cls._TEMPLATE \
            .replace(
                "RESPONSE_DATETIME",
                datetime.now().isoformat()
            ) \
            .replace(
                "DATE",
                datetime.now().strftime("%Y-%m-%d")
            ) \
            .replace(
                "IDENTIFIER",
                identifier or "test:oai_dc:" + str(uuid4())
            ) \
            .replace(
                "CREATOR",
                creator
            ) \
            .replace(
                "SUBJECT",
                subject
            ) \
            .replace(
                "INSTITUTION",
                institution
            ) \
            .replace(
                "TITLE",
                title
            )

    def get(self, **kwargs) -> PluginResult:
        result = PluginResult(log=Logger(default_origin=self._NAME))
        self.set_progress(
            verbose="Plugin starting up", numeric=0, status=Status.RUNNING
        )

        identifiers = kwargs["number"]

        for idx, _ in enumerate(range(identifiers)):
            identifier = "test:oai_dc:" + str(uuid4())
            self.set_progress(
                verbose=f"collecting record '{identifier}' "
                + f"({idx + 1}/{kwargs['number']})",
                numeric=int(100*idx/kwargs["number"])
            )
            ie_path = self._get_ie_output()
            (ie_path / "meta").mkdir()
            (ie_path / "meta" / "source_metadata.xml").write_text(
                self.generate_metadata(
                    kwargs["randomize"], identifier
                ),
                encoding="utf-8"
            )
            (ie_path / "data" / "preservation_master").mkdir(parents=True)
            (
                ie_path / "data" / "preservation_master" / "payload.txt"
            ).write_text(
                "called with: " + str(kwargs),
                encoding="utf-8"
            )
            result.ies["ie"+str(idx).zfill(len(str(identifiers)))] = \
                IE(
                    path=ie_path,
                    source_identifier=identifier,
                    fetched_payload=True
                )
            result.log.log(
                Context.INFO,
                body=f"Created IE in '{ie_path}'.",
                origin=self._NAME
            )
        self.set_progress(
            verbose="all records processed",
            numeric=100,
            status=Status.COMPLETED
        )
        return result
