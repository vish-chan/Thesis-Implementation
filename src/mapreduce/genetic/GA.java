package mapreduce.genetic;

import sun.rmi.runtime.Log;

public class GA {

    public static Individual mInit(byte[] newSolution) {

        // Set a candidate solution
        FitnessCalc.setSolution(newSolution);

        // Create an initial population
        Population myPop = new Population(50, true);
        
        // Evolve our population until we reach an optimum solution
        int generationCount = 0;
        while (myPop.getFittest().getFitness() < FitnessCalc.getMaxFitness()) {
            generationCount++;
            Log.printLine("Generation: " + generationCount + " Fittest: " + myPop.getFittest().getFitness());
            myPop = Algorithm.evolvePopulation(myPop);
        }
        return  myPop.getFittest();
    }
}
