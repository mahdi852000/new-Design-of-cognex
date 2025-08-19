package org.example.akka.metrics;

import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.markers.SeriesMarkers;

import java.nio.file.*;
import java.util.*;

public class ChartFromCsv {
    public static void main(String[] args) throws Exception {
        Path csv = Paths.get(args.length > 0 ? args[0] : "metrics.csv");
        Path out = Paths.get(args.length > 1 ? args[1] : "metrics.png");

        List<String> lines = Files.readAllLines(csv);
        if (lines.isEmpty()) { System.out.println("CSV is empty: " + csv.toAbsolutePath()); return; }

        String head = lines.get(0).replace("\uFEFF", "");
        String sep  = head.contains("\t") ? "\t" : (head.contains(";") ? ";" : ",");
        String sepRe = "\\s*" + java.util.regex.Pattern.quote(sep) + "\\s*";
        String[] h = head.split(sepRe, -1);

        //For Debug
        System.out.println("Header: " + java.util.Arrays.toString(h));

        int iTs  = idx(h, "ts");
        int iAvg = idx(h, "avg", "avg(5)", "average");
        int iMea = idx(h, "measurement", "meas", "value");
        int iOcc = idx(h, "occupied");
        int iLat = idx(h, "dmccLatencyMs");

        if (iAvg < 0 || iMea < 0) {
            System.err.println("Required columns not found. Header = " + java.util.Arrays.toString(h));
            return;
        }


        List<Double> x   = new ArrayList<>();
        List<Double> avg = new ArrayList<>();
        List<Double> mea = new ArrayList<>();


        List<Double> xOcc = new ArrayList<>();
        List<Double> occ  = new ArrayList<>();

        List<Double> xLat = new ArrayList<>();
        List<Double> lat  = new ArrayList<>();

        double t0 = -1;
        int sampleIdx = 0;

        for (int i = 1; i < lines.size(); i++) {
            String[] c = lines.get(i).split(sepRe,-1);
            if (c.length <= Math.max(iAvg, iMea)) continue;

            // --- time (relative) ---
            double xi;
            if (iTs >= 0) {
                try {
                    long ts = Long.parseLong(c[iTs].trim());
                    double s = ts / 1000.0;
                    if (t0 < 0) t0 = s;
                    xi = s - t0;
                } catch (Exception e) { xi = sampleIdx; }
            } else xi = sampleIdx;


            x.add(xi);
            avg.add(Double.parseDouble(c[iAvg].trim()));
            mea.add(Double.parseDouble(c[iMea].trim()));


            if (iOcc >= 0 && iOcc < c.length) {
                String s = c[iOcc].trim();
                if (!s.isEmpty()) {
                    boolean o = "true".equalsIgnoreCase(s);
                    xOcc.add(xi);
                    occ.add(o ? 90.3 : 89.7);
                }
            }


            if (iLat >= 0 && iLat < c.length) {
                String s = c[iLat].trim();
                if (!s.isEmpty()) {
                    try {
                        double v = Double.parseDouble(s);
                        if (v >= 0) { xLat.add(xi); lat.add(v); }
                    } catch (Exception ignore) {}
                }
            }

            sampleIdx++;
        }

        XYChart chart = new XYChartBuilder()
                .width(1400).height(600)
                .title("Metrics")
                .xAxisTitle(iTs >= 0 ? "time (s from start)" : "sample #")
                .yAxisTitle("value")
                .build();


        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideE);
        chart.getStyler().setChartTitleVisible(true);


        XYSeries sAvg = chart.addSeries("avg(5)", x, avg);
        sAvg.setMarker(SeriesMarkers.NONE);

        XYSeries sMea = chart.addSeries("measurement", x, mea);

        sMea.setMarker(SeriesMarkers.CIRCLE);


        if (!occ.isEmpty()) {
            XYSeries sOcc = chart.addSeries("occupied (90±0.3)", xOcc, occ);
            sOcc.setMarker(SeriesMarkers.DIAMOND);
            sOcc.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        }


        if (!lat.isEmpty()) {
            chart.getStyler().setYAxisGroupPosition(1, Styler.YAxisPosition.Right);
            XYSeries sLat = chart.addSeries("dmccLatencyMs", xLat, lat);
            sLat.setYAxisGroup(1);
            sLat.setMarker(SeriesMarkers.CROSS);
            sLat.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Scatter);
        }

        String outPath = out.toString();
        if (outPath.toLowerCase().endsWith(".png")) outPath = outPath.substring(0, outPath.length() - 4);
        BitmapEncoder.saveBitmap(chart, outPath, BitmapEncoder.BitmapFormat.PNG);
        System.out.println("Saved PNG -> " + out.toAbsolutePath());
    }

    static int idx(String[] h, String... names) {
        for (int i = 0; i < h.length; i++) {
            String col = h[i].trim().toLowerCase();
            for (String n : names) if (col.equals(n.toLowerCase())) return i;
        }
        return -1;
    }

}
