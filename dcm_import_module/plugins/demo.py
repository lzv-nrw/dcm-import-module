"""Demo IE-import plugin."""

from typing import Optional
from datetime import datetime
from uuid import uuid4
import random

from dcm_common.logger import LoggingContext
from dcm_common.plugins import Signature, Argument, JSONType

from dcm_import_module.models import IE
from .interface import IEImportPlugin, IEImportResult, IEImportContext


class DemoPlugin(IEImportPlugin):
    """
    Demo IE-import plugin generating fake data for use in demonstrations
    and tests.
    """

    _DISPLAY_NAME = "Demo-Plugin"
    _NAME = "demo"
    _DESCRIPTION = "Fake IE generation."
    _DEPENDENCIES = []

    _SIGNATURE = Signature(
        test=IEImportPlugin.signature.properties["test"],
        number=Argument(
            type_=JSONType.INTEGER,
            required=False,
            description="number of generated IEs",
            example=2,
            default=1,
        ),
        randomize=Argument(
            type_=JSONType.BOOLEAN,
            required=False,
            description="randomize generated metadata",
            example=True,
            default=False,
        ),
        bad_ies=Argument(
            type_=JSONType.BOOLEAN,
            required=False,
            description="include invalid IEs",
            example=True,
            default=False,
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
        "Featherstonehaugh",
        "Thistlethwaite",
        "Crumpetsworth",
        "Bottombean",
    ]
    _SUBJECTS = [
        "Quantum Entanglement",
        "Nanotechnology",
        "Number Theory",
        "Dynamical Systems",
        "Human Physiology",
        "Linguistics",
        "Political Science",
    ]
    _INSTITUTIONS = [
        "Harvard University",
        "Massachusetts Institute of Technology (MIT)",
        "University of Oxford",
        "Stanford University",
        "Princeton University",
        "Yale University",
    ]
    _TITLES = [
        "Dancing {} and the Quantum {}",
        "Whispers from the {}: Unveiling the secrets of {}",
        "Decoding the {}: When a {} changes the world",
        "Symphony of the {}: Mapping the Music of {}",
        "Unmasking the {}: Understanding the Enigmas of {}",
    ]
    _WORDS = [
        "Photons",
        "Butterfly Effect",
        "Time",
        "Brain",
        "Consciousness",
        "Fractals",
        "Chaos",
        "Law",
        "Humanity",
        "Genome",
        "Language",
    ]

    @classmethod
    def generate_metadata(
        cls, randomize: bool = False, identifier: Optional[str] = None
    ) -> str:
        """Generate fake metadata based on template."""
        creator = (
            (
                random.choice(cls._SURNAMES)
                + ", "
                + random.choice(cls._FIRSTNAMES)
            )
            if randomize
            else (cls._SURNAMES[0] + ", " + cls._FIRSTNAMES[0])
        )
        subject = (
            random.choice(cls._SUBJECTS) if randomize else cls._SUBJECTS[0]
        )
        institution = (
            random.choice(cls._INSTITUTIONS)
            if randomize
            else cls._INSTITUTIONS[0]
        )
        _title = random.choice(cls._TITLES) if randomize else cls._TITLES[0]
        title = (
            _title.format(random.choice(cls._WORDS), random.choice(cls._WORDS))
            if randomize
            else _title.format(cls._WORDS[0], cls._WORDS[1])
        )
        return (
            cls._TEMPLATE.replace(
                "RESPONSE_DATETIME", datetime.now().isoformat()
            )
            .replace("DATE", datetime.now().strftime("%Y-%m-%d"))
            .replace("IDENTIFIER", identifier or "test:oai_dc:" + str(uuid4()))
            .replace("CREATOR", creator)
            .replace("SUBJECT", subject)
            .replace("INSTITUTION", institution)
            .replace("TITLE", title)
        )

    def _generate_ie(
        self, identifier: str, fetched_payload: bool, kwargs
    ) -> IE:
        """Generates and returns IE."""
        ie_path = self._get_ie_output()
        (ie_path / "meta").mkdir()
        (ie_path / "meta" / "source_metadata.xml").write_text(
            self.generate_metadata(kwargs["randomize"], identifier),
            encoding="utf-8",
        )
        (ie_path / "data" / "preservation_master").mkdir(parents=True)
        (ie_path / "data" / "preservation_master" / "payload.txt").write_text(
            "called with: " + str(kwargs), encoding="utf-8"
        )
        return IE(
            path=ie_path,
            source_identifier=identifier,
            fetched_payload=fetched_payload,
        )

    def _get(self, context: IEImportContext, /, **kwargs) -> IEImportResult:
        # initialize
        context.result.log.log(LoggingContext.INFO, body="Starting to generate IEs.")
        context.set_progress("generating IEs")
        context.push()

        # iterate
        nidentifiers = kwargs["number"]
        for idx, _ in enumerate(range(nidentifiers)):
            identifier = "test:oai_dc:" + str(uuid4())
            ie_id = "ie" + str(idx).zfill(len(str(nidentifiers)))

            context.set_progress(f"generating record '{identifier}' ({ie_id})")
            context.push()

            context.result.ies[ie_id] = self._generate_ie(
                identifier, idx % 2 == 0 if kwargs["bad_ies"] else True, kwargs
            )
            context.result.log.log(
                LoggingContext.INFO,
                body=f"Created IE in '{context.result.ies[ie_id].path}'.",
                origin=self._NAME,
            )
            if not context.result.ies[ie_id].fetched_payload:
                context.result.log.log(
                    LoggingContext.ERROR,
                    body=f"Missing payload in IE '{ie_id}'.",
                    origin=self._NAME,
                )
            context.push()

        # eval
        context.result.success = all(
            ie.fetched_payload for ie in context.result.ies.values()
        )
        if context.result.success:
            context.set_progress("success")
        else:
            context.set_progress("failure")
        context.push()
        return context.result
