import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Testing {
    public static void main(String args[]) {
        try {
            System.out.println("hello world");
            ProcessBuilder processBuilder = new ProcessBuilder();

            //processBuilder.command("bash", "-c", "wget --http-user=" + "sensad" + " --http-password=" + "i’mUsingThisUniquePWH3R3" + " https://audio.broadcastify.com/27730.mp3 > " + "testing.mp3");
            processBuilder.command("./testing.sh");
            Process process = processBuilder.start();
            long t = System.currentTimeMillis();
            while (System.currentTimeMillis() - t <= 1 * 1000) {
                //do nothing
            }
            //then kill the process and upload the file to S3

            StringBuilder output = new StringBuilder();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line + "\n");
            }
            process.destroy();

            System.out.println(output);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
