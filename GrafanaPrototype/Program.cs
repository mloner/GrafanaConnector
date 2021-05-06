using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration.Attributes;
using Prometheus;

namespace GrafanaPrototype
{
    class Program
    {
        public class Record
        {
            public string Customer { get; set; }
            public string Service { get; set; }
            public string instanceId { get; set; }
            public string City { get; set; }
            public string Building { get; set; }
            public string Measurement { get; set; }
            public string Stage { get; set; }
            public string location { get; set; }
            public DateTime ExpectedDatetime { get; set; }
            public DateTime DeliveryDatetime { get; set; }
            public string ExpectedDQ { get; set; }
            public string factDQ { get; set; }

            public void piece(string line)
            {
                string[] parts = line.Split(';');  //Разделитель в CVS файле.
                Customer = parts[0];
                Service = parts[1];
                instanceId = parts[2];
                City = parts[3];
                Building = parts[4];
                Measurement = parts[5];
                Stage = parts[6];
                location = parts[7];
                ExpectedDatetime = DateTime.ParseExact(parts[8], "dd.MM.yyyy HH:mm", CultureInfo.InvariantCulture);
                DeliveryDatetime = DateTime.ParseExact(parts[9], "dd.MM.yyyy HH:mm", CultureInfo.InvariantCulture);
                ExpectedDQ = parts[10];
                factDQ = parts[11];
            }
            public static List<Record> ReadFile(string filename)
            {
                List<Record> res = new List<Record>();
                using (StreamReader sr = new StreamReader(filename))
                {
                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
                        Record p = new Record();
                        p.piece(line);
                        res.Add(p);
                    }
                }
                return res;
            }

        }
        public class Model1
        {
            public string Customer { get; set; }
            public string Service { get; set; }
            public string instanceId { get; set; }
            public DateTime dateTime { get; set; }
            public int importDoneCount { get; set; }
            public int importTotalCount { get; set; }
            public double importStatus { get; set; }
            public string importStatusString { get; set; }
            public int processingDoneCount { get; set; }
            public int processingTotalCount { get; set; }
            public double processingStatus { get; set; }
            public string processingStatusString { get; set; }
            public int exportDoneCount { get; set; }
            public int exportTotalCount { get; set; }
            public double exportStatus { get; set; }
            public string exportStatusString { get; set; }
            public int doneCount { get; set; }
            public int totalCount { get; set; }
            public double status { get; set; }
            public string statusString { get; set; }
            public static double colorThresholdYellow = 0.50;
            public static double colorThresholdGreen = 0.80;
            public static string getColor(double status)
            {
                if (status < colorThresholdYellow)
                    return "Red";
                else if (status < colorThresholdGreen)
                    return "Yellow";
                else
                    return "Green";
            }
        }
        public class Model2
        {
            public string Customer { get; set; }
            public string Service { get; set; }
            public string instanceId { get; set; }
            public DateTime dateTime { get; set; }
            public string measurement { get; set; }
            public string status { get; set; }
            public string statusGlobal { get; set; }
            public string color { get; set; }
            public static string getColorByStatus(string status)
            {
                if (status.EndsWith("done"))
                    return "Green";
                else
                    return "Yellow";
            }
            public static string getIdByStatus(string status)
            {
                Dictionary<string, string> statusIds = new Dictionary<string, string>()
                {
                    { "import process",     "0" },
                    { "processing process", "1" },   
                    { "export process",  "2" },
                    { "import done",        "3" },
                    { "processing done",    "4" },
                    { "export done",        "5" }
                };
                return statusIds[status];
            }
        }
        public static void sendModel1(List<Record> recs)
        {
            // сгруппировать данные по Customer, Service, instanceId
            var qry = recs.GroupBy(q => new { q.Customer, q.Service, q.instanceId });
            // лист с записями для prometheus
            List<Model1> model1Records = new List<Model1>();

            // подсчитать сумму полей
            foreach (var el in qry)
            {
                Console.WriteLine(el.Key.Customer);
                List<DateTime> dtimes = new List<DateTime>();
                int importDoneCount = 0,
                    importTotalCount = 0,
                    processingDoneCount = 0,
                    processingTotalCount = 0,
                    exportDoneCount = 0,
                    exportTotalCount = 0,
                    doneCount = 0,
                    totalCount = 0;
                string instId = el.Key.instanceId;
                foreach (var el2 in el)
                {
                    dtimes.Add(el2.DeliveryDatetime);
                    switch (el2.Stage)
                    {
                        case "import done":
                            importDoneCount++;
                            break;
                        case "processing done":
                            processingDoneCount++;
                            break;
                        case "export done":
                            exportDoneCount++;
                            break;
                    }
                    if (el2.Stage.StartsWith("import"))
                        importTotalCount++;
                    else if (el2.Stage.StartsWith("processing"))
                        processingTotalCount++;
                    else if (el2.Stage.StartsWith("export"))
                        exportTotalCount++;
                    if (el2.Stage.EndsWith("done"))
                        doneCount++;
                    totalCount++;

                }
                dtimes.Sort();
                double importStatus = 0,
                       processingStatus = 0,
                       exportStatus = 0,
                       status = 0;
                importStatus = (double)importDoneCount / importTotalCount;
                processingStatus = (double)processingDoneCount / processingTotalCount;
                exportStatus = (double)exportDoneCount / exportTotalCount;
                status = (double)doneCount / totalCount;
                model1Records.Add(new Model1()
                {
                    Customer = el.Key.Customer,
                    Service = el.Key.Service,
                    dateTime = dtimes.Last(),
                    importDoneCount = importDoneCount,
                    importTotalCount = importTotalCount,
                    importStatus = importStatus,
                    importStatusString = importDoneCount.ToString() + "/" + importTotalCount.ToString(),
                    processingDoneCount = processingDoneCount,
                    processingTotalCount = processingTotalCount,
                    processingStatus = processingStatus,
                    processingStatusString = processingDoneCount.ToString() + "/" + processingTotalCount.ToString(),
                    exportDoneCount = exportDoneCount,
                    exportTotalCount = exportTotalCount,
                    exportStatus = exportStatus,
                    exportStatusString = exportDoneCount.ToString() + "/" + exportTotalCount.ToString(),
                    doneCount = doneCount,
                    totalCount = totalCount,
                    status = status,
                    statusString = doneCount.ToString() + "/" + totalCount.ToString(),
                    instanceId = instId
                });
                Console.WriteLine();
                Console.WriteLine();
            }

                // метрика для  prometheus
                Gauge model1 = Metrics.CreateGauge("model1", "",
            new GaugeConfiguration
            {
                LabelNames = new[] {
                    "Customer",
                    "Service",
                    "instanceId",
                    "DateTime",
                    "importDoneCount",
                    "importTotalCount",
                    "importStatus",
                    "importStatusString",
                    "processingDoneCount",
                    "processingTotalCount",
                    "processingStatus",
                    "processingStatusString",
                    "exportDoneCount",
                    "exportTotalCount",
                    "exportStatus",
                    "exportStatusString",
                    "doneCount",
                    "totalCount",
                    "status",
                    "statusString",
                    "color"
                }
            });

            // отправить метрики в prometheus
            foreach (Model1 item in model1Records)
            {
                model1.WithLabels(
                    item.Customer,
                    item.Service,
                    item.instanceId,
                    item.dateTime.ToString(),
                    Math.Round((double)item.importDoneCount, 2).ToString().Replace(",", "."),
                    Math.Round((double)item.importTotalCount, 2).ToString().Replace(",", "."),
                    Math.Round((double)item.importStatus, 2).ToString().Replace(",", "."),
                    item.importStatusString,
                    Math.Round((double)item.processingDoneCount, 2).ToString().Replace(",", "."),
                    Math.Round((double)item.processingTotalCount, 2).ToString().Replace(",", "."),
                    Math.Round((double)item.processingStatus, 2).ToString().Replace(",", "."),
                    item.processingStatusString,
                    Math.Round((double)item.exportDoneCount, 2).ToString().Replace(",", "."),
                    Math.Round((double)item.exportTotalCount, 2).ToString().Replace(",", "."),
                    Math.Round((double)item.exportStatus, 2).ToString().Replace(",", "."),
                    item.exportStatusString,
                    Math.Round((double)item.doneCount, 2).ToString().Replace(",", "."),
                    Math.Round((double)item.totalCount, 2).ToString().Replace(",", "."),
                    Math.Round((double)item.status, 2).ToString().Replace(",", "."),
                    item.statusString,
                    Model1.getColor(item.status)
                    ).Set(1);
            }
        }

        public static void sendModel2(List<Record> recs)
        {

            // сгруппировать данные по Customer, Service, instanceId
            var qry = recs.GroupBy(q => new { q.Customer, q.Service, q.instanceId });
            // лист с записями для prometheus
            List<Model2> model2Records = new List<Model2>();

            // подсчитать сумму полей
            foreach (var el in qry)
            {
                Console.WriteLine(el.Key.Customer);
                List<DateTime> dtimes = new List<DateTime>();
                int importDoneCount = 0,
                    importTotalCount = 0,
                    processingDoneCount = 0,
                    processingTotalCount = 0,
                    exportDoneCount = 0,
                    exportTotalCount = 0,
                    doneCount = 0,
                    totalCount = 0;
                foreach (var el2 in el)
                {
                    switch (el2.Stage)
                    {
                        case "import done":
                            importDoneCount++;
                            break;
                        case "processing done":
                            processingDoneCount++;
                            break;
                        case "export done":
                            exportDoneCount++;
                            break;
                    }
                    if (el2.Stage.StartsWith("import"))
                        importTotalCount++;
                    else if (el2.Stage.StartsWith("processing"))
                        processingTotalCount++;
                    else if (el2.Stage.StartsWith("export"))
                        exportTotalCount++;
                    if (el2.Stage.EndsWith("done"))
                        doneCount++;
                    totalCount++;
                    model2Records.Add(new Model2()
                    {
                        Customer = el2.Customer,
                        Service = el2.Service,
                        dateTime = el2.DeliveryDatetime,
                        instanceId = el2.instanceId,
                        measurement = el2.Measurement,
                        status = el2.Stage,
                        color = Model2.getColorByStatus(el2.Stage),
                        statusGlobal = "-1"
                    }) ;
                }
                double importStatus = 0,
                       processingStatus = 0,
                       exportStatus = 0,
                       status = 0;
                importStatus = (double)importDoneCount / importTotalCount;
                processingStatus = (double)processingDoneCount / processingTotalCount;
                exportStatus = (double)exportDoneCount / exportTotalCount;
                status = (double)doneCount / totalCount;
                foreach (Model2 r in model2Records)
                {
                    if (r.statusGlobal == "-1")
                        r.statusGlobal = status.ToString();
                }
                Console.WriteLine();
                Console.WriteLine();
            }


            // метрика для  prometheus
            Gauge model2 = Metrics.CreateGauge("model2", "",
                new GaugeConfiguration
                {
                    LabelNames = new[] {
                            "Customer",
                            "Service",
                            "instanceId",
                            "DateTime",
                            "measurement",
                            "status",
                            "statusGlobal",
                            "colorGlobal",
                            "statusId",
                            "color"
                    }
                });

            // отправить метрики в prometheus
            foreach (Model2 item in model2Records)
            {
                model2.WithLabels(
                    item.Customer,
                    item.Service,
                    item.instanceId,
                    item.dateTime.ToString(),
                    item.measurement,
                    item.status,
                    item.statusGlobal,
                    Model1.getColor(Convert.ToDouble(item.statusGlobal)),
                    Model2.getIdByStatus(item.status),
                    item.color
                    ).Set(1);
            }
        }



        static void Main(string[] args)
        {
            //получить данные из таблицы
            List<Record> recs = Record.ReadFile("data.csv");

            // prometheus server
            var server = new MetricServer(hostname: "localhost", port: 1234);
            server.Start();

            sendModel1(recs);
            sendModel2(recs);

            Console.WriteLine("Done");
            Console.Read();
        }
    }
}
