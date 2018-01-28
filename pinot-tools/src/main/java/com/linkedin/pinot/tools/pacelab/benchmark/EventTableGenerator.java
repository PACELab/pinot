/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.tools.pacelab.benchmark;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.controller.helix.core.sharding.LatencyBasedLoadMetric;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.extractors.FieldExtractor;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.tools.admin.command.EventTableCreationCommand;
import com.linkedin.pinot.tools.data.generator.RangeIntGenerator;
import com.linkedin.pinot.tools.data.generator.RangeLongGenerator;
import com.linkedin.pinot.tools.data.generator.SchemaAnnotation;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.IntRange;
import org.apache.commons.lang.math.LongRange;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class EventTableGenerator {
    private String _dataDir;
    private String _outDir;
    private boolean _overwrite = true;
    private int _numRecords = 10;
    final String _profileSchemaFile = "pinot_benchmark_schemas/ProfileSchema.json";
    final String _profileViewSchemaFile = "pinot_benchmark_schemas/ProfileViewSchema.json";
    final String _profileViewSchemaAnnFile = "pinot_benchmark_schemas/ProfileViewSchemaAnnotation.json";

    final String _adSchemaFile = "pinot_benchmark_schemas/AdSchema.json";
    final String _adClickSchemaFile = "pinot_benchmark_schemas/AdClickSchema.json";
    final String _adClickSchemaAnnFile = "pinot_benchmark_schemas/AdClickSchemaAnnotation.json";

    final String _jobSchemaFile = "pinot_benchmark_schemas/JobSchema.json";
    final String _jobApplySchemaFile = "pinot_benchmark_schemas/JobApplySchema.json";
    final String _jobApplySchemaAnnFile = "pinot_benchmark_schemas/JobApplySchemaAnnotation.json";

    final String _articleSchemaFile = "pinot_benchmark_schemas/ArticleSchema.json";
    final String _articleReadSchemaFile = "pinot_benchmark_schemas/ArticleReadSchema.json";
    final String _articleReadSchemaAnnFile = "pinot_benchmark_schemas/ArticleReadSchemaAnnotation.json";

    public EventTableGenerator(String dataDir, String outDir)
    {
        _dataDir = dataDir;
        _outDir = outDir;
    }

    private String getTableDataDirectory(String tableName)
    {
        String  tableDatDir;
        if(_dataDir.endsWith("/"))
        {
            tableDatDir = _dataDir + tableName;
        }
        else
        {
            tableDatDir = _dataDir + tableName;
        }

        // Filter out all input files.
        File dir = new File(tableDatDir);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new RuntimeException("Data directory " + tableDatDir + " not found.");
        }

        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith("avro");
            }
        });

        if ((files == null) || (files.length == 0)) {
            throw new RuntimeException(
                    "Data directory " + _dataDir + " does not contain " + "avro" + " files.");
        }

        return  files[0].getAbsolutePath();
    }

    private List<GenericRow> readBaseTableData(String schemaFile, String dataFile) throws Exception
    {
         List<GenericRow> tableData = new ArrayList<GenericRow>();

        Schema profileSchema = Schema.fromFile(new File(schemaFile));
        FieldExtractor extractor = FieldExtractorFactory.getPlainFieldExtractor(profileSchema);
        RecordReader reader =  new AvroRecordReader(extractor, dataFile);
        reader.init();
        while(reader.hasNext())
        {
            tableData.add(reader.next());
        }
        return tableData;
    }

    private List<SchemaAnnotation> readSchemaAnnotationFile(String schemaAnnotationFile) throws Exception
    {
        ObjectMapper objectMapper = new ObjectMapper();
        List<SchemaAnnotation> saList = objectMapper.readValue(new File(schemaAnnotationFile), new TypeReference<List<SchemaAnnotation>>() {});
        return saList;
    }

    private RangeLongGenerator createLongRangeGenerator(List<SchemaAnnotation> saList, String columnName)
    {
        long end = System.currentTimeMillis()/1000;
        long start = end - (30*24*3600);

        for (SchemaAnnotation sa : saList) {
            String column = sa.getColumn();
            if(column.equalsIgnoreCase(columnName))
            {
                start = sa.getRangeStart();
                end = sa.getRangeEnd();
            }
        }
        return  new RangeLongGenerator(start, end);
    }

    private RangeIntGenerator createIntRangeGenerator(List<SchemaAnnotation> saList, String columnName)
    {
        int end = 1800;
        int start = 1;

        for (SchemaAnnotation sa : saList) {
            String column = sa.getColumn();
            if(column.equalsIgnoreCase(columnName))
            {
                start = sa.getRangeStart();
                end = sa.getRangeEnd();
            }
        }
        return  new RangeIntGenerator(start, end);
    }

    private File createOutDirAndFile(String outDirName) throws Exception
    {
        String outDirPath;
        if(_outDir.endsWith("/"))
        {
            outDirPath = _outDir + outDirName;
        }
        else
        {
            outDirPath = _outDir + "/" + outDirName;
        }
        // Make sure output directory does not already exist, or can be overwritten.
        File outDir = new File(outDirPath);
        if (outDir.exists()) {
            if (!_overwrite) {
                throw new IOException("Output directory " + outDirPath + " already exists.");
            } else {
                FileUtils.deleteDirectory(outDir);
            }
        }

        outDir.mkdir();


        return new File(outDirPath, "part-" + 0 + ".avro");
    }

    private DataFileWriter<GenericData.Record> createRecordWriter(String schemaFile, File avroFile) throws Exception
    {
        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(schemaFile))).toString());
        final GenericDatumWriter<GenericData.Record> datum = new GenericDatumWriter<GenericData.Record>(schemaJSON);
        DataFileWriter<GenericData.Record> recordWriter = new DataFileWriter<GenericData.Record>(datum);
        recordWriter.create(schemaJSON, avroFile);
        return recordWriter;

    }


    public boolean generateProfileViewTable() throws Exception
    {

        ClassLoader classLoader = LatencyBasedLoadMetric.class.getClassLoader();
        String profileSchemaFile = classLoader.getResource(_profileSchemaFile).getFile();
        String profileViewSchemaFile = classLoader.getResource(_profileViewSchemaFile).getFile();
        String profileViewSchemaAnn = classLoader.getResource(_profileViewSchemaAnnFile).getFile();

        String profileDataFile = getTableDataDirectory("profile");
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);
        List<SchemaAnnotation> saList = readSchemaAnnotationFile(profileViewSchemaAnn);
        RangeLongGenerator eventTimeGenerator = createLongRangeGenerator(saList,"ViewStartTime");
        RangeIntGenerator timeSpentGenerator = createIntRangeGenerator(saList,"ReviewTime");
        File avroFile = createOutDirAndFile("profile-view-data");
        DataFileWriter<GenericData.Record> recordWriter = createRecordWriter(profileViewSchemaFile,avroFile);

        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(profileViewSchemaFile))).toString());

        for(int i=0;i<_numRecords;i++)
        {
            final GenericData.Record outRecord = new GenericData.Record(schemaJSON);
            GenericRow viewerProfile = getRandomGenericRow(profileTable);
            GenericRow viewedProfile = getRandomGenericRow(profileTable);
            while(viewedProfile == viewerProfile)
            {
                viewedProfile = getRandomGenericRow(profileTable);
            }

            outRecord.put("ViewStartTime", eventTimeGenerator.next());
            outRecord.put("ReviewTime",timeSpentGenerator.next());
            outRecord.put("ViewerProfileId", viewerProfile.getValue("ID"));
            outRecord.put("ViewerCompany", viewerProfile.getValue("Company"));
            outRecord.put("ViewerHeadline", viewerProfile.getValue("Headline"));
            outRecord.put("ViewerPosition", viewerProfile.getValue("Position"));
            outRecord.put("ViewedProfileId", viewedProfile.getValue("ID"));
            outRecord.put("ViewedProfileHeadline", viewedProfile.getValue("Headline"));
            outRecord.put("ViewedProfilePosition", viewedProfile.getValue("Position"));
            outRecord.put("WereProfilesConnected", randomYesOrNo());

            recordWriter.append(outRecord);
        }

        recordWriter.close();
        return true;
    }

    public boolean generateAdClickTable() throws Exception
    {
        ClassLoader classLoader = LatencyBasedLoadMetric.class.getClassLoader();
        String profileSchemaFile = classLoader.getResource(_profileSchemaFile).getFile();
        String adSchemaFile = classLoader.getResource(_adSchemaFile).getFile();
        String adClickSchemaAnn = classLoader.getResource(_adClickSchemaAnnFile).getFile();
        String adClickSchemaFile = classLoader.getResource(_adClickSchemaFile).getFile();

        String profileDataFile = getTableDataDirectory("profile");
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);

        String adDataFile = getTableDataDirectory("ad");
        List<GenericRow> adTable = readBaseTableData(adSchemaFile,adDataFile);

        List<SchemaAnnotation> saList = readSchemaAnnotationFile(adClickSchemaAnn);
        RangeLongGenerator eventTimeGenerator = createLongRangeGenerator(saList,"ClickTime");
        File avroFile = createOutDirAndFile("ad-click-data");
        DataFileWriter<GenericData.Record> recordWriter = createRecordWriter(adClickSchemaFile,avroFile);

        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(adClickSchemaFile))).toString());

        for(int i=0;i<_numRecords;i++)
        {
            final GenericData.Record outRecord = new GenericData.Record(schemaJSON);

            GenericRow viewerProfile = getRandomGenericRow(profileTable);
            GenericRow adInfo = getRandomGenericRow(adTable);

            outRecord.put("ClickTime", eventTimeGenerator.next());
            outRecord.put("ViewerProfileId", viewerProfile.getValue("ID"));
            outRecord.put("ViewerHeadline", viewerProfile.getValue("Headline"));
            outRecord.put("ViewerPosition", viewerProfile.getValue("Position"));
            outRecord.put("AdID", adInfo.getValue("ID"));
            outRecord.put("AdUrl", adInfo.getValue("Url"));
            outRecord.put("AdTitle", adInfo.getValue("Title"));
            outRecord.put("AdText", adInfo.getValue("Text"));
            outRecord.put("AdCompany", adInfo.getValue("Company"));
            outRecord.put("AdCampaign", adInfo.getValue("Campaign"));

            recordWriter.append(outRecord);
        }

        recordWriter.close();
        return true;
    }

    public boolean generateJobApplyTable() throws Exception
    {
        ClassLoader classLoader = LatencyBasedLoadMetric.class.getClassLoader();
        String profileSchemaFile = classLoader.getResource(_profileSchemaFile).getFile();
        String jobSchemaFile = classLoader.getResource(_jobSchemaFile).getFile();
        String jobApplySchemaAnn = classLoader.getResource(_jobApplySchemaAnnFile).getFile();
        String jobApplySchemaFile = classLoader.getResource(_jobApplySchemaFile).getFile();

        String profileDataFile = getTableDataDirectory("profile");
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);

        String jobDataFile = getTableDataDirectory("job");
        List<GenericRow> jobTable = readBaseTableData(jobSchemaFile,jobDataFile);

        List<SchemaAnnotation> saList = readSchemaAnnotationFile(jobApplySchemaAnn);
        RangeLongGenerator eventTimeGenerator = createLongRangeGenerator(saList,"ApplyStartTime");
        RangeIntGenerator timeSpentGenerator = createIntRangeGenerator(saList,"TimeSpent");

        File avroFile = createOutDirAndFile("job-apply-data");
        DataFileWriter<GenericData.Record> recordWriter = createRecordWriter(jobApplySchemaFile,avroFile);

        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(jobApplySchemaFile))).toString());

        for(int i=0;i<_numRecords;i++)
        {
            final GenericData.Record outRecord = new GenericData.Record(schemaJSON);

            GenericRow applicantProfile = getRandomGenericRow(profileTable);
            GenericRow jobInfo = getRandomGenericRow(jobTable);

            outRecord.put("ApplyStartTime", eventTimeGenerator.next());
            outRecord.put("TimeSpent", timeSpentGenerator.next());
            outRecord.put("JobSalary", jobInfo.getValue("Salary"));
            outRecord.put("ApplicantProfileId", applicantProfile.getValue("ID"));
            outRecord.put("ApplicantHeadline", applicantProfile.getValue("Headline"));
            outRecord.put("ApplicantPosition", applicantProfile.getValue("Position"));
            outRecord.put("JobID", jobInfo.getValue("ID"));
            outRecord.put("JobUrl", jobInfo.getValue("Url"));
            outRecord.put("JobTitle", jobInfo.getValue("Title"));
            outRecord.put("JobCompany", jobInfo.getValue("Company"));
            outRecord.put("DidApplyIsFinalized", randomYesOrNo());

            recordWriter.append(outRecord);
        }

        recordWriter.close();
        return true;
    }

    public boolean generateArticleReadTable() throws Exception
    {
        ClassLoader classLoader = LatencyBasedLoadMetric.class.getClassLoader();
        String profileSchemaFile = classLoader.getResource(_profileSchemaFile).getFile();
        String articleSchemaFile = classLoader.getResource(_articleSchemaFile).getFile();
        String articleReadSchemaAnn = classLoader.getResource(_articleReadSchemaAnnFile).getFile();
        String articleReadSchemaFile = classLoader.getResource(_articleReadSchemaFile).getFile();

        String profileDataFile = getTableDataDirectory("profile");
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);

        String articleDataFile = getTableDataDirectory("article");
        List<GenericRow> articleTable = readBaseTableData(articleSchemaFile, articleDataFile);

        List<SchemaAnnotation> saList = readSchemaAnnotationFile(articleReadSchemaAnn);
        RangeLongGenerator eventTimeGenerator = createLongRangeGenerator(saList,"ReadStartTime");
        RangeIntGenerator timeSpentGenerator = createIntRangeGenerator(saList,"TimeSpent");

        File avroFile = createOutDirAndFile("article-read-data");
        DataFileWriter<GenericData.Record> recordWriter = createRecordWriter(articleReadSchemaFile,avroFile);

        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(articleReadSchemaFile))).toString());

        for(int i=0;i<_numRecords;i++)
        {
            final GenericData.Record outRecord = new GenericData.Record(schemaJSON);

            GenericRow readerProfile = getRandomGenericRow(profileTable);
            GenericRow articleInfo = getRandomGenericRow(articleTable);

            outRecord.put("ReadStartTime", eventTimeGenerator.next());
            outRecord.put("TimeSepnt", timeSpentGenerator.next());
            outRecord.put("ReaderProfileId", readerProfile.getValue("ID"));
            outRecord.put("ReaderHeadline", readerProfile.getValue("Headline"));
            outRecord.put("ReaderPosition", readerProfile.getValue("Position"));
            outRecord.put("ArticleID", articleInfo.getValue("ID"));
            outRecord.put("ArticleUrl", articleInfo.getValue("Url"));
            outRecord.put("ArticleTitle", articleInfo.getValue("Title"));
            outRecord.put("ArticleAbstract", articleInfo.getValue("Abstract"));
            outRecord.put("ArticleAuthor", articleInfo.getValue("Author"));
            outRecord.put("ArticleCompany", articleInfo.getValue("Company"));
            outRecord.put("ArticleTopic", articleInfo.getValue("Topic"));

            recordWriter.append(outRecord);
        }

        recordWriter.close();
        return true;
    }


    private String randomYesOrNo()
    {
        Random randGen = new Random(System.currentTimeMillis());
        int index = randGen.nextInt(1);
        if(index == 1)
            return "Yes";
        else
            return "NO";
    }

    private GenericRow getRandomGenericRow(List<GenericRow> rowList)
    {
        int size = rowList.size();
        Random randGen = new Random(System.currentTimeMillis());
        int index = randGen.nextInt(size);
        return rowList.get(index);
    }

    public JSONObject getJSONSchema(Schema schema) throws JSONException {
        final JSONObject ret = new JSONObject();
        ret.put("name", "data_gen_record");
        ret.put("type", "record");

        final JSONArray fields = new JSONArray();

        for (final FieldSpec spec : schema.getAllFieldSpecs()) {
            fields.put(spec.getDataType().toJSONSchemaFor(spec.getName()));
        }

        ret.put("fields", fields);

        return ret;
    }
}

