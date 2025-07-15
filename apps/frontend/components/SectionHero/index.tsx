import { Grid } from "lucide-react";
import { GridContainer } from "../GridContainer";
import { TagFeature } from "./TagFeature";
import { AreaCtas } from "./AreaCtas";

export function SectionHero() {
  return (
    <section className="pt-24">
        <GridContainer>
            <div className="text-center">
                <TagFeature />
                <h1 className="text-6xl font-semibold text-gray-100 mt-4 mb-6">Beautiful analytics to grow smarter</h1>
                <p className="text-xl/6 text-gray-400 max-w-3xl mx-auto mb-12 ">Lorem, ipsum dolor sit amet consectetur adipisicing elit. Ratione aspernatur sed voluptas, iure suscipit esse aliquam voluptatum illo neque vel quod soluta impedit quasi! Odit tempore obcaecati optio iure dignissimos.</p>
               <AreaCtas/>
            </div>
        </GridContainer>
    </section>
  );
}